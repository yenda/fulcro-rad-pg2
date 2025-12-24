(ns com.fulcrologic.rad.database-adapters.sql.resolvers-pathom3
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.authorization :as auth]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.malli-transform :as mt]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [com.fulcrologic.rad.database-adapters.sql.query :as sql.query]
   [com.fulcrologic.rad.database-adapters.sql.resolvers :as resolvers]
   [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
   [com.wsscode.pathom3.connect.operation :as pco]
   [honey.sql :as sql]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]))

(defn reorder-maps-by-id
  "Reorder target-vec to match the order of order-vec based on id-key.
   Maps in order-vec without a corresponding entry in target-vec are preserved as-is."
  [id-key order-vec target-vec]
  (let [id-map (into {} (map (juxt id-key identity)) target-vec)]
    (mapv #(get id-map (id-key %) %) order-vec)))

(defn get-column [attribute]
  (let [col (or (::rad.sql/column-name attribute)
                (:column attribute)
                (some-> (::attr/qualified-key attribute) name))]
    (if col
      (keyword col)
      (throw (ex-info "Can't find column name for attribute" {:attribute attribute})))))

(defn get-table [attribute]
  (some-> (::rad.sql/table attribute) keyword))

(defn get-pg-array-type
  "Get the PostgreSQL array type string for an attribute's type.
   Used for prepared statement optimization with = ANY($1::type[]) syntax."
  [{::attr/keys [type]}]
  (case type
    :uuid "uuid[]"
    :int "int4[]"
    :long "int8[]"
    :string "text[]"
    ;; Default to text[] which PostgreSQL can often coerce
    "text[]"))

(defn to-one-keyword [qualified-key]
  (keyword (str (namespace qualified-key)
                "/"
                (name qualified-key)
                "-id")))

(defn get-column->outputs
  [id-attr-k id-attr->attributes k->attr target?]
  (let [{::attr/keys [cardinality qualified-key target] :as id-attribute} (k->attr id-attr-k)
        outputs (reduce (fn [acc {::attr/keys [cardinality qualified-key target]
                                  ::rad.sql/keys [ref] :as attr}]
                          (if (or (= :many cardinality)
                                  (and (= :one cardinality) (and ref (not target?))))
                            acc
                            (let [column (get-column attr)
                                  outputs (if cardinality
                                            [(to-one-keyword qualified-key)
                                             {qualified-key [target]}]
                                            [qualified-key])]
                              (assoc acc column outputs))))
                        {(get-column id-attribute) [qualified-key]}
                        (id-attr->attributes id-attribute))]
    outputs))

(defn get-column-mapping [column->outputs]
  (reduce-kv (fn [acc column outputs]
               (reduce (fn [acc output]
                         (if (keyword? output)
                           (conj acc [[output] column])
                           (conj acc [(vec (flatten (vec output))) column])))
                       acc
                       outputs))
             []
             column->outputs))

(defn get-column->attr
  "Build a mapping from column keyword to attribute for value transformation."
  [id-attr id-attr->attributes k->attr]
  (let [id-column (get-column id-attr)]
    (reduce (fn [acc attr]
              (assoc acc (get-column attr) attr))
            {id-column id-attr}
            (id-attr->attributes id-attr))))

(defn compile-row-decoder-for-resolver
  "Compile a Malli-based row decoder for a set of attributes.

   This creates a compiled decoder at resolver generation time that efficiently
   transforms all columns in a single pass at query time.

   Arguments:
     column->attr - Map from column keyword to attribute

   Returns:
     A function (fn [row] transformed-row) that applies SQL->Clojure transformations."
  [column->attr]
  (let [attrs (vals column->attr)]
    (mt/compile-row-decoder attrs
                            ::attr/qualified-key
                            (fn [attr] (::rad.sql/column-name attr))
                            ::attr/type)))

(defn build-pg2-column-config
  "Build the configuration for pg2 zero-copy row transformation.

   Maps pg2 column keywords (e.g., :created_at) directly to output paths
   and types, enabling single-pass transformation.

   Arguments:
     column-mapping - Vector of [[output-path] column] pairs
     column->attr   - Map from column keyword to attribute

   Returns:
     Map like {:created_at {:output-path [:message/created-at] :type :instant}}"
  [column-mapping column->attr]
  (reduce
   (fn [acc [output-path column]]
     (let [attr (get column->attr column)
           attr-type (when attr (::attr/type attr))]
       (assoc acc column {:output-path output-path
                          :type attr-type})))
   {}
   column-mapping))

(defn id-resolver [{::attr/keys [id-attr id-attr->attributes attributes k->attr] :as c}]
  (let [id-attr-k (::attr/qualified-key id-attr)
        column->outputs (get-column->outputs id-attr-k id-attr->attributes k->attr false)
        column-mapping (get-column-mapping column->outputs)
        column->attr (get-column->attr id-attr id-attr->attributes k->attr)
        ;; Compile the row decoder once at resolver generation time
        decode-row (compile-row-decoder-for-resolver column->attr)
        ;; pg2 zero-copy: compile transformer that goes directly from pg2 raw -> resolver output
        pg2-column-config (build-pg2-column-config column-mapping column->attr)
        pg2-transform-row (mt/compile-pg2-row-transformer pg2-column-config)
        outputs (vec (flatten (vals column->outputs)))
        columns (keys column->outputs)
        table (get-table id-attr)
        id-column (get-column id-attr)
        ;; pg2 prepared statement: pre-compile SQL at resolver generation time
        ;; Uses = ANY($1::type[]) for 100% prepared statement cache hit rate
        pg2-prepared-sql (format "SELECT %s FROM %s WHERE %s = ANY($1::%s)"
                                 (str/join ", " (map name columns))
                                 (name table)
                                 (name id-column)
                                 (get-pg-array-type id-attr))]
    (log/debug "Generating resolver for id key" id-attr-k
               "to resolve" outputs)
    (let [{::attr/keys [schema]
           ::pco/keys [transform]} id-attr
          op-name (symbol
                   (str (namespace id-attr-k))
                   (str (name id-attr-k) "-resolver"))
          id-resolver
          (pco/resolver
           op-name
           (cond->
            {::pco/output outputs
             ::pco/batch? true
             ::pco/resolve
             (fn [env input]
               (sql.query/timer
                "id-resolver"
                (let [ids (mapv id-attr-k input)
                      pool-wrapper (get-in env [::rad.sql/connection-pools schema])
                      pg2? (= :pg2 (:driver pool-wrapper))
                      ;; Use pg2 fast path when available
                      results-by-id
                      (if pg2?
                        ;; pg2 prepared statement path: pre-compiled SQL + array param
                        (let [rows (pg2/pg2-query-prepared! (:pool pool-wrapper)
                                                            pg2-prepared-sql
                                                            ids)]
                          (reduce
                           (fn [acc row]
                             (let [result (pg2-transform-row row)]
                               (assoc acc (id-attr-k result) result)))
                           {}
                           rows))
                        ;; Standard path: HoneySQL + eql-query! + decode + column-mapping
                        (let [query (sql/format {:select columns
                                                 :from table
                                                 :where [:in id-column ids]})
                              rows (sql.query/eql-query! env query schema input)]
                          (reduce
                           (fn [acc row]
                             (let [decoded-row (decode-row row)
                                   result (reduce
                                           (fn [acc [output-path column]]
                                             (let [value (get decoded-row column)]
                                               (if (and (nil? value) (= (count output-path) 2))
                                                 acc
                                                 (assoc-in acc output-path value))))
                                           {}
                                           column-mapping)]
                               (assoc acc (id-attr-k result) result)))
                           {}
                           rows)))
                      results (mapv (fn [id] (get results-by-id id)) ids)]
                  (auth/redact env results))
                {:op-name op-name}))
             ::pco/input [id-attr-k]}
             transform (assoc ::pco/transform transform)))]
      id-resolver)))

(defn to-many-resolvers
  [{::attr/keys [schema]
    ::rad.sql/keys [ref] ;; user/organization
    ::pco/keys [resolve transform] :as attr
    attr-k ::attr/qualified-key ;; organization/users
    target-k ::attr/target} ;; user/id
   id-attr-k ;; organization/id
   id-attr->attributes
   k->attr]
  (when-not resolve
    (when-not ref
      (throw (ex-info "Missing ref in to-many ref" attr)))
    (let [op-name (symbol
                   (str (namespace attr-k))
                   (str (name attr-k) "-resolver"))
          _ (log/debug "Building Pathom3 resolver" op-name "for" attr-k "by" id-attr-k)
          target-attr (or (k->attr target-k)
                          (throw (ex-info "Target attribute not found" attr)))
          table (get-table target-attr)
          ref-attr (or (k->attr ref)
                       (throw (ex-info "Ref attribute not found" attr)))
          _ (when (= (::attr/cardinality ref-attr)
                     :many)
              (throw (ex-info "Many to many relations are not implemented" {:target-attr target-attr
                                                                            :ref-attr ref-attr})))
          relationship-column (get-column ref-attr)
          target-column (get-column target-attr)
          order-by (some-> (::rad.sql/order-by attr)
                           k->attr
                           get-column)
          select [[relationship-column :k] [[:array_agg (if order-by
                                                          [:order-by target-column order-by]
                                                          target-column)] :v]]
          ;; pg2 prepared statement: pre-compile SQL at resolver generation time
          id-attr (k->attr id-attr-k)
          pg2-prepared-sql (format "SELECT %s AS k, array_agg(%s%s) AS v FROM %s WHERE %s = ANY($1::%s) GROUP BY %s"
                                   (name relationship-column)
                                   (name target-column)
                                   (if order-by (str " ORDER BY " (name order-by)) "")
                                   (name table)
                                   (name relationship-column)
                                   (get-pg-array-type id-attr)
                                   (name relationship-column))
          entity-by-attribute-resolver
          (pco/resolver
           op-name
           (cond-> {::pco/output [{attr-k [target-k]}]
                    ::pco/batch? true
                    ::pco/resolve
                    (fn [env input]
                      (sql.query/timer
                       "to-many-resolver"
                       (let [ids (mapv id-attr-k input)
                             pool-wrapper (get-in env [::rad.sql/connection-pools schema])
                             pg2? (= :pg2 (:driver pool-wrapper))
                             ;; pg2 path: query returns {:k uuid :v [uuid...]} directly
                             ;; No type transformation needed for UUIDs
                             rows (if pg2?
                                    (pg2/pg2-query-prepared! (:pool pool-wrapper) pg2-prepared-sql ids)
                                    (let [query (sql/format
                                                 {:select select
                                                  :from table
                                                  :where [:in relationship-column ids]
                                                  :group-by [relationship-column]})]
                                      (sql.query/eql-query! env query schema input)))
                             results-by-id (reduce (fn [acc {:keys [k v]}]
                                                     (assoc acc k
                                                            {attr-k (mapv (fn [v]
                                                                            {target-k v})
                                                                          v)}))
                                                   {}
                                                   rows)
                             results (mapv #(get results-by-id %) ids)]

                         (auth/redact env results))
                       {:op-name op-name}))
                    ::pco/input [id-attr-k]}
             transform (assoc ::pco/transform transform)))]
      entity-by-attribute-resolver)))

(defn to-one-resolvers [{::attr/keys [schema]
                         ::pco/keys [transform] :as attr
                         ::rad.sql/keys [ref] ;; question.index-card/question
                         attr-k ::attr/qualified-key ;; :question/index-card
                         target-k ::attr/target} ;; question.index-card/id
                        id-attr-k ;; question/id
                        id-attr->attributes
                        k->attr]
  (if-not ref
    ;; when there is no ref that means that the table has a
    ;; column with the ref.
    ;; so we only need to alias that ref attribute to reuse
    ;; the id-resolver of the ref entity.
    (pbir/alias-resolver (to-one-keyword attr-k) target-k)
    (let [{::attr/keys [schema]
           ::pco/keys [transform]} attr
          ref-attr (k->attr ref)
          target-attr (k->attr target-k)
          column->outputs (get-column->outputs target-k id-attr->attributes k->attr true)
          column-mapping (get-column-mapping column->outputs)
          column->attr (get-column->attr target-attr id-attr->attributes k->attr)
          ;; Compile the row decoder once at resolver generation time
          decode-row (compile-row-decoder-for-resolver column->attr)
          ;; pg2 zero-copy: compile transformer that goes directly from pg2 raw -> resolver output
          pg2-column-config (build-pg2-column-config column-mapping column->attr)
          pg2-transform-row (mt/compile-pg2-row-transformer pg2-column-config)
          outputs (vec (flatten (vals column->outputs)))
          columns (keys column->outputs)
          table (get-table target-attr)
          ref-column (get-column ref-attr)
          ;; pg2 prepared statement: pre-compile SQL at resolver generation time
          id-attr (k->attr id-attr-k)
          pg2-prepared-sql (format "SELECT %s FROM %s WHERE %s = ANY($1::%s)"
                                   (str/join ", " (map name columns))
                                   (name table)
                                   (name ref-column)
                                   (get-pg-array-type id-attr))
          op-name (symbol
                   (str (namespace target-k))
                   (str "by-" (namespace id-attr-k) "-" (name id-attr-k) "-resolver"))
          one-to-one-resolver
          (pco/resolver
           op-name
           (cond->
            {::pco/output (conj outputs {attr-k outputs})
             ::pco/batch? true
             ::pco/resolve
             (fn [env input]
               (sql.query/timer
                "to-one-resolver"
                (let [ids (mapv id-attr-k input)
                      pool-wrapper (get-in env [::rad.sql/connection-pools schema])
                      pg2? (= :pg2 (:driver pool-wrapper))
                      ;; Transform all SQL values in one pass
                      results-by-id
                      (if pg2?
                        ;; pg2 prepared statement path: pre-compiled SQL + array param
                        (let [rows (pg2/pg2-query-prepared! (:pool pool-wrapper)
                                                            pg2-prepared-sql
                                                            ids)]
                          (reduce
                           (fn [acc row]
                             (let [result (pg2-transform-row row)]
                               (assoc acc (get-in result [ref id-attr-k]) result)))
                           {}
                           rows))
                        ;; Standard path: HoneySQL + eql-query! + decode + column-mapping
                        (let [query (sql/format {:select columns
                                                 :from table
                                                 :where [:in ref-column ids]})
                              rows (sql.query/eql-query! env query schema input)]
                          (reduce
                           (fn [acc row]
                             (let [decoded-row (decode-row row)
                                   result
                                   (reduce
                                    (fn [acc [output-path column]]
                                      (let [value (get decoded-row column)]
                                        (assoc-in acc output-path value)))
                                    {}
                                    column-mapping)]
                               (assoc acc (get-in result [ref id-attr-k]) result)))
                           {}
                           rows)))
                      results (mapv (fn [id]
                                      (when-let [outputs (get results-by-id id)]
                                        (assoc outputs attr-k outputs)))
                                    ids)]
                  (auth/redact env results))
                {:op-name op-name}))
             ::pco/input [id-attr-k]}
             transform (assoc ::pco/transform transform)))]
      one-to-one-resolver)))

(defn generate-resolvers
  "Returns a sequence of resolvers that can resolve attributes from
  SQL databases."
  [attributes schema]
  (log/info "Generating resolvers for SQL schema" schema)
  (let [k->attr (enc/keys-by ::attr/qualified-key attributes)
        id-attr->attributes (->> attributes
                                 (filter #(= schema (::attr/schema %)))
                                 (mapcat
                                  (fn [attribute]
                                    (for [entity-id (::attr/identities attribute)]
                                      (assoc attribute ::entity-id (k->attr entity-id)))))
                                 (group-by ::entity-id))
        ;; id-resolvers are basically a get by id, but they also handle batches, so that
        ;; one can ask for more than one id in a single query.
        ;; - we build one id resolver for each id attribute defined
        ;; - each resolvers grabs all the attributes defined for that particular identity
        ;; in the select clause.
        _ (log/debug "Generating resolvers for id attributes")
        id-resolvers
        (reduce-kv
         (fn [resolvers id-attr attributes]
           (conj resolvers (id-resolver {::attr/id-attr id-attr
                                         ::attr/id-attr->attributes id-attr->attributes
                                         ::attr/attributes attributes
                                         ::attr/k->attr k->attr})))
         []
         id-attr->attributes)

        ;; there are two types of target resolvers: one and many
        ;;
        target-resolvers
        (->> attributes
             (filter #(and
                       (= schema (::attr/schema %))
                       (#{:one :many} (::attr/cardinality %))))
             (mapcat
              (fn [{::attr/keys [cardinality] :as attribute}]
                (for [id-attr-k (::attr/identities attribute)]
                  (case cardinality
                    :one (to-one-resolvers attribute
                                           id-attr-k
                                           id-attr->attributes
                                           k->attr)
                    :many (to-many-resolvers attribute
                                             id-attr-k
                                             id-attr->attributes
                                             k->attr)))))
             ;; removing the resolvers that were not built.  for
             ;; instance it could be a manually written one that
             ;; doesn't need to be generated for a particular to-many
             ;; relationship
             (remove nil?))]
    (vec (concat id-resolvers target-resolvers))))
