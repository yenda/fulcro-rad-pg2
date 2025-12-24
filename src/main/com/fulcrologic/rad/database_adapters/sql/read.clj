(ns com.fulcrologic.rad.database-adapters.sql.read
  "Pathom3 read resolvers for pg2 PostgreSQL driver."
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string :as str]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.authorization :as auth]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
   [com.wsscode.pathom3.connect.operation :as pco]
   [malli.experimental :as mx]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Column/Table Helpers

(defn get-column
  "Extract SQL column name from attribute.
   Checks ::rad.sql/column-name, then derives from qualified-key name with snake_case.
   Returns a keyword for HoneySQL compatibility."
  [attribute]
  (let [col (or (::rad.sql/column-name attribute)
                (some-> (::attr/qualified-key attribute) name csk/->snake_case))]
    (if col
      (keyword col)
      (throw (ex-info "Can't find column name for attribute"
                      {:attribute attribute
                       :qualified-key (::attr/qualified-key attribute)
                       :type :missing-column})))))

(defn get-table
  "Extract SQL table name from attribute's ::rad.sql/table.
   Returns a keyword for HoneySQL compatibility, or nil if not set."
  [attribute]
  (some-> (::rad.sql/table attribute) keyword))

(defn get-pg-array-type
  "Get the PostgreSQL array type string for an attribute's type.
   Used for prepared statement optimization with = ANY($1::type[]) syntax."
  [{::attr/keys [type]}]
  (case type
    :uuid "uuid[]"
    :int "int4[]"
    :long "int8[]"
    :boolean "boolean[]"
    :decimal "numeric[]"
    :instant "timestamptz[]"
    (:keyword :enum :string) "text[]"
    "text[]"))

(defn to-one-keyword
  "Convert a qualified ref keyword to its FK column keyword by appending -id.
   E.g., :user/address -> :user/address-id
   Used for forward refs where the FK column exists on the source table."
  [qualified-key]
  (keyword (str (namespace qualified-key)
                "/"
                (name qualified-key)
                "-id")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Column Mapping

(defn get-column->outputs
  "Build mapping from SQL column keyword to Pathom output specification.
   For scalar attributes: {:col [:attr-k]}
   For to-one refs: {:col [:attr-id-k {:attr-k [:target-k]}]}
   Skips to-many refs and reverse refs (handled by separate resolvers)."
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

(defn get-column-mapping
  "Transform column->outputs map to seq of [output-path column] pairs.
   Used to build the pg2 column config for row transformation.

   For scalar outputs: [[[:attr-key] :column]]
   For ref outputs with nested target: [[[:ref-key :target-key] :column]]"
  [column->outputs]
  (reduce-kv (fn [acc column outputs]
               (reduce (fn [acc output]
                         (if (keyword? output)
                           (conj acc [[output] column])
                           ;; For map entries like {:ref-key [:target-key]}, flatten to [:ref-key :target-key]
                           (conj acc [(vec (flatten (seq output))) column])))
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

(defn build-pg2-column-config
  "Build the configuration for pg2 zero-copy row transformation.
   Maps pg2 column keywords directly to output paths and types."
  [column-mapping column->attr]
  (reduce
   (fn [acc [output-path column]]
     (let [attr (get column->attr column)
           attr-type (when attr (::attr/type attr))]
       (assoc acc column {:output-path output-path
                          :type attr-type})))
   {}
   column-mapping))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pool Access

(defn get-pool
  "Get connection pool for schema from env, throwing if not found."
  [env schema]
  (or (get-in env [::rad.sql/connection-pools schema])
      (throw (ex-info "No connection pool configured for schema"
                      {:schema schema
                       :available-schemas (keys (::rad.sql/connection-pools env))
                       :type :missing-pool}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ID Resolver

(defn id-resolver
  "Generate a batch resolver for fetching entities by their ID attribute.
   Returns a Pathom3 resolver that:
   - Takes a vector of entity IDs as input
   - Executes a single batch SQL query with = ANY($1::type[])
   - Returns results in the same order as input IDs (nil for missing)"
  [{::attr/keys [id-attr id-attr->attributes k->attr]}]
  (let [id-attr-k (::attr/qualified-key id-attr)
        column->outputs (get-column->outputs id-attr-k id-attr->attributes k->attr false)
        column-mapping (get-column-mapping column->outputs)
        column->attr (get-column->attr id-attr id-attr->attributes k->attr)
        pg2-column-config (build-pg2-column-config column-mapping column->attr)
        pg2-transform-row (pg2/compile-pg2-row-transformer pg2-column-config)
        outputs (into [] cat (vals column->outputs))
        columns (keys column->outputs)
        table (get-table id-attr)
        id-column (get-column id-attr)
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
               (let [ids (mapv id-attr-k input)]
                 (when (seq ids)
                   (pg2/timer
                    "id-resolver"
                    (let [pool (get-pool env schema)
                          rows (pg2/pg2-query-prepared! pool pg2-prepared-sql ids)
                          results-by-id (reduce
                                         (fn [acc row]
                                           (let [result (pg2-transform-row row)]
                                             (assoc acc (id-attr-k result) result)))
                                         {}
                                         rows)]
                      (auth/redact env (mapv results-by-id ids)))
                    {:op-name op-name}))))
             ::pco/input [id-attr-k]}
             transform (assoc ::pco/transform transform)))]
      id-resolver)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; To-Many Resolver

(defn to-many-resolvers
  "Generate a batch resolver for to-many (reverse) references.
   The target table has a FK column pointing back to the source entity.
   Uses array_agg to fetch all related IDs in a single query."
  [{::attr/keys [schema]
    ::rad.sql/keys [ref order-by]
    ::pco/keys [resolve transform]
    attr-k ::attr/qualified-key
    target-k ::attr/target}
   id-attr-k
   _id-attr->attributes
   k->attr]
  (when-not resolve
    (when-not ref
      (throw (ex-info (str "Missing ::rad.sql/ref for to-many attribute " attr-k)
                      {:attr-key attr-k :type :missing-ref})))
    (let [op-name (symbol
                   (str (namespace attr-k))
                   (str (name attr-k) "-resolver"))
          _ (log/debug "Building Pathom3 resolver" op-name "for" attr-k "by" id-attr-k)
          target-attr (or (k->attr target-k)
                          (throw (ex-info (str "Target attribute " target-k " not found for " attr-k)
                                          {:attr-key attr-k :target target-k :type :missing-target})))
          table (get-table target-attr)
          ref-attr (or (k->attr ref)
                       (throw (ex-info (str "Ref attribute " ref " not found for " attr-k)
                                       {:attr-key attr-k :ref ref :type :missing-ref-attr})))
          _ (when (= (::attr/cardinality ref-attr) :many)
              (throw (ex-info "Many to many relations are not implemented"
                              {:attr-key attr-k :target-attr target-k :ref-attr ref :type :unsupported})))
          relationship-column (get-column ref-attr)
          target-column (get-column target-attr)
          order-by-column (some-> order-by k->attr get-column)
          id-attr (k->attr id-attr-k)
          pg2-prepared-sql (format "SELECT %s AS k, array_agg(%s%s) AS v FROM %s WHERE %s = ANY($1::%s) GROUP BY %s"
                                   (name relationship-column)
                                   (name target-column)
                                   (if order-by-column (str " ORDER BY " (name order-by-column)) "")
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
                      (let [ids (mapv id-attr-k input)]
                        (when (seq ids)
                          (pg2/timer
                           "to-many-resolver"
                           (let [pool (get-pool env schema)
                                 rows (pg2/pg2-query-prepared! pool pg2-prepared-sql ids)
                                 results-by-id (reduce (fn [acc {:keys [k v]}]
                                                         (assoc acc k
                                                                {attr-k (mapv (fn [v]
                                                                                {target-k v})
                                                                              v)}))
                                                       {}
                                                       rows)]
                             (auth/redact env (mapv results-by-id ids)))
                           {:op-name op-name}))))
                    ::pco/input [id-attr-k]}
             transform (assoc ::pco/transform transform)))]
      entity-by-attribute-resolver)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; To-One Resolver

(defn to-one-resolvers
  "Generate a resolver for to-one references.
   Two cases:
   1. Forward ref (no ::rad.sql/ref): Source table has FK column, use alias resolver
   2. Reverse ref (has ::rad.sql/ref): Target table has FK, generate batch resolver"
  [{::attr/keys [schema]
    ::pco/keys [transform]
    ::rad.sql/keys [ref]
    attr-k ::attr/qualified-key
    target-k ::attr/target}
   id-attr-k
   id-attr->attributes
   k->attr]
  (if-not ref
    ;; Forward ref: source table has FK column.
    ;; Alias to reuse the id-resolver of the target entity.
    (pbir/alias-resolver (to-one-keyword attr-k) target-k)
    ;; Reverse ref: target table has FK pointing to source.
    (let [ref-attr (k->attr ref)
          target-attr (k->attr target-k)
          column->outputs (get-column->outputs target-k id-attr->attributes k->attr true)
          column-mapping (get-column-mapping column->outputs)
          column->attr (get-column->attr target-attr id-attr->attributes k->attr)
          pg2-column-config (build-pg2-column-config column-mapping column->attr)
          pg2-transform-row (pg2/compile-pg2-row-transformer pg2-column-config)
          outputs (into [] cat (vals column->outputs))
          columns (keys column->outputs)
          table (get-table target-attr)
          ref-column (get-column ref-attr)
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
               (let [ids (mapv id-attr-k input)]
                 (when (seq ids)
                   (pg2/timer
                    "to-one-resolver"
                    (let [pool (get-pool env schema)
                          rows (pg2/pg2-query-prepared! pool pg2-prepared-sql ids)
                          results-by-id (reduce
                                         (fn [acc row]
                                           (let [result (pg2-transform-row row)]
                                             (assoc acc (get-in result [ref id-attr-k]) result)))
                                         {}
                                         rows)]
                      ;; Return nil for missing entities - Pathom3 batch semantics
                      (auth/redact env (mapv (fn [id]
                                               (when-let [result (results-by-id id)]
                                                 (assoc result attr-k result)))
                                             ids)))
                    {:op-name op-name}))))
             ::pco/input [id-attr-k]}
             transform (assoc ::pco/transform transform)))]
      one-to-one-resolver)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Resolver Generation

(mx/defn generate-resolvers :- [:vector :some]
  "Returns a sequence of resolvers that can resolve attributes from SQL databases."
  [attributes :- [:sequential :map]
   schema :- :keyword]
  (log/info "Generating resolvers for SQL schema" schema)
  (let [k->attr (enc/keys-by ::attr/qualified-key attributes)
        id-attr->attributes (->> attributes
                                 (filter #(= schema (::attr/schema %)))
                                 (mapcat
                                  (fn [attribute]
                                    (for [entity-id (::attr/identities attribute)]
                                      (assoc attribute ::entity-id (k->attr entity-id)))))
                                 (group-by ::entity-id))
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
             (remove nil?))]
    (vec (concat id-resolvers target-resolvers))))
