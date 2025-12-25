(ns com.fulcrologic.rad.database-adapters.pg2.read
  "Pathom3 read resolvers for pg2 PostgreSQL driver."
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string :as str]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.authorization :as auth]
   [com.fulcrologic.rad.database-adapters.pg2 :as rad.pg2]
   [com.fulcrologic.rad.database-adapters.pg2.driver :as pg2]
   [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
   [com.wsscode.pathom3.connect.operation :as pco]
   [malli.experimental :as mx]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Column/Table Helpers

(defn get-column
  "Extract SQL column name from attribute.
   Checks ::rad.pg2/column-name, then derives from qualified-key name with snake_case.
   Returns a keyword for HoneySQL compatibility."
  [attribute]
  (let [col (or (::rad.pg2/column-name attribute)
                (some-> (::attr/qualified-key attribute) name csk/->snake_case))]
    (if col
      (keyword col)
      (throw (ex-info "Can't find column name for attribute"
                      {:attribute attribute
                       :qualified-key (::attr/qualified-key attribute)
                       :type :missing-column})))))

(defn get-table
  "Extract SQL table name from attribute's ::rad.pg2/table.
   Returns a keyword for HoneySQL compatibility, or nil if not set."
  [attribute]
  (some-> (::rad.pg2/table attribute) keyword))

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
;; SQL Builders (pure functions, easily testable)
;;
;; NOTE: These functions use Clojure's `format` for SQL generation instead of
;; HoneySQL (used in write.clj). This is intentional: read queries are simple,
;; static SELECT patterns where string formatting is faster than building and
;; serializing a HoneySQL data structure in the hot read path.

(defn build-select-by-ids-sql
  "Build a SELECT statement for batch ID lookup.
   Returns: SELECT col1, col2 FROM table WHERE id_col = ANY($1::type[])"
  [columns table id-column pg-array-type]
  (format "SELECT %s FROM %s WHERE %s = ANY($1::%s)"
          (str/join ", " (map name columns))
          (name table)
          (name id-column)
          pg-array-type))

(defn build-array-agg-sql
  "Build a SELECT with array_agg for to-many relationships.
   Returns: SELECT fk AS k, array_agg(target ORDER BY order_col) AS v FROM table WHERE fk = ANY($1::type[]) GROUP BY fk"
  [relationship-column target-column table pg-array-type order-by-column]
  (format "SELECT %s AS k, array_agg(%s%s) AS v FROM %s WHERE %s = ANY($1::%s) GROUP BY %s"
          (name relationship-column)
          (name target-column)
          (if order-by-column (str " ORDER BY " (name order-by-column)) "")
          (name table)
          (name relationship-column)
          pg-array-type
          (name relationship-column)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Resolver Name Helpers

(defn make-id-resolver-name
  "Generate resolver name for ID resolver: namespace/name-resolver"
  [id-attr-k]
  (symbol (namespace id-attr-k)
          (str (name id-attr-k) "-resolver")))

(defn make-to-many-resolver-name
  "Generate resolver name for to-many resolver: namespace/name-resolver"
  [attr-k]
  (symbol (namespace attr-k)
          (str (name attr-k) "-resolver")))

(defn make-to-one-resolver-name
  "Generate resolver name for reverse to-one resolver: target-ns/by-id-ns-id-name-resolver"
  [target-k id-attr-k]
  (symbol (namespace target-k)
          (str "by-" (namespace id-attr-k) "-" (name id-attr-k) "-resolver")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Column Mapping (pure functions for building pg2 config)

(defn get-column->outputs
  "Build mapping from SQL column keyword to Pathom output specification.
   For scalar attributes: {:col [:attr-k]}
   For to-one refs: {:col [:attr-id-k {:attr-k [:target-k]}]}
   Skips to-many refs and reverse refs (handled by separate resolvers)."
  [id-attr-k id-attr->attributes k->attr target?]
  (let [{::attr/keys [qualified-key] :as id-attribute} (k->attr id-attr-k)]
    (reduce (fn [acc {::attr/keys [cardinality qualified-key target]
                      ::rad.pg2/keys [ref] :as attr}]
              (if (or (= :many cardinality)
                      (and (= :one cardinality) ref (not target?)))
                acc
                (let [column (get-column attr)
                      outputs (if cardinality
                                [(to-one-keyword qualified-key)
                                 {qualified-key [target]}]
                                [qualified-key])]
                  (assoc acc column outputs))))
            {(get-column id-attribute) [qualified-key]}
            (id-attr->attributes id-attribute))))

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
  [id-attr id-attr->attributes _k->attr]
  (let [id-column (get-column id-attr)]
    (reduce (fn [acc attr]
              (assoc acc (get-column attr) attr))
            {id-column id-attr}
            (id-attr->attributes id-attr))))

(defn build-pg2-column-config
  "Build the configuration for pg2 zero-copy row transformation.
   Maps pg2 column keywords directly to output paths, types, and custom transformers."
  [column-mapping column->attr]
  (reduce
   (fn [acc [output-path column]]
     (let [attr (get column->attr column)
           attr-type (when attr (::attr/type attr))
           ;; Include custom sql->form-value transformer if defined
           sql->form-value (when attr (::rad.pg2/sql->form-value attr))]
       (assoc acc column (cond-> {:output-path output-path
                                  :type attr-type}
                           sql->form-value (assoc :sql->form-value sql->form-value)))))
   {}
   column-mapping))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Runtime Helpers (pure functions for query result processing)

(defn get-pool
  "Get connection pool for schema from env, throwing if not found."
  [env schema]
  (or (get-in env [::rad.pg2/connection-pools schema])
      (throw (ex-info "No connection pool configured for schema"
                      {:schema schema
                       :available-schemas (keys (::rad.pg2/connection-pools env))
                       :type :missing-pool}))))

(defn index-rows-by-key
  "Index query results by a key extracted from each row.
   key-fn: extracts the index key from the raw row
   value-fn: transforms the raw row to the indexed value"
  [rows key-fn value-fn]
  (reduce (fn [acc row]
            (assoc acc (key-fn row) (value-fn row)))
          {}
          rows))

(defn order-results-by-ids
  "Return results in the same order as input IDs. Missing IDs get nil."
  [results-by-id ids]
  (mapv results-by-id ids))

(defn order-results-by-ids-with-wrapper
  "Return results in the same order as input IDs, applying wrapper-fn to non-nil results."
  [results-by-id ids wrapper-fn]
  (mapv (fn [id]
          (when-let [result (results-by-id id)]
            (wrapper-fn result)))
        ids))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Resolver Config Builders (pure functions that compute all generation-time values)

(defn build-id-resolver-config
  "Build all configuration needed for an ID resolver at generation time.
   Returns a map with :sql, :id-column, :transform-row, :outputs, :op-name, :schema"
  [{::attr/keys [id-attr id-attr->attributes k->attr]}]
  (let [id-attr-k (::attr/qualified-key id-attr)
        column->outputs (get-column->outputs id-attr-k id-attr->attributes k->attr false)
        column-mapping (get-column-mapping column->outputs)
        column->attr (get-column->attr id-attr id-attr->attributes k->attr)
        pg2-column-config (build-pg2-column-config column-mapping column->attr)
        columns (keys column->outputs)
        table (get-table id-attr)
        id-column (get-column id-attr)]
    {:sql (build-select-by-ids-sql columns table id-column (get-pg-array-type id-attr))
     :id-column id-column
     :id-attr-k id-attr-k
     :transform-row (pg2/compile-pg2-row-transformer pg2-column-config)
     :outputs (into [] cat (vals column->outputs))
     :op-name (make-id-resolver-name id-attr-k)
     :schema (::attr/schema id-attr)
     :transform (::pco/transform id-attr)}))

(defn build-to-many-resolver-config
  "Build all configuration needed for a to-many resolver at generation time.
   Returns a map with :sql, :wrap-targets, :outputs, :op-name, :schema, or nil if invalid."
  [{::rad.pg2/keys [fk-attr order-by]
    ::pco/keys [resolve transform]
    attr-k ::attr/qualified-key
    target-k ::attr/target}
   id-attr-k
   k->attr]
  (when-not resolve
    (when-not fk-attr
      (throw (ex-info (str "Missing ::rad.pg2/fk-attr for to-many attribute " attr-k)
                      {:attr-key attr-k :type :missing-fk-attr})))
    (let [target-attr (or (k->attr target-k)
                          (throw (ex-info (str "Target attribute " target-k " not found for " attr-k)
                                          {:attr-key attr-k :target target-k :type :missing-target})))
          fk-attr-def (or (k->attr fk-attr)
                          (throw (ex-info (str "FK attribute " fk-attr " not found for " attr-k)
                                          {:attr-key attr-k :fk-attr fk-attr :type :missing-fk-attr-def})))
          _ (when (= (::attr/cardinality fk-attr-def) :many)
              (throw (ex-info "Many to many relations are not implemented"
                              {:attr-key attr-k :target-attr target-k :fk-attr fk-attr :type :unsupported})))
          id-attr (k->attr id-attr-k)]
      {:sql (build-array-agg-sql
             (get-column fk-attr-def)
             (get-column target-attr)
             (get-table target-attr)
             (get-pg-array-type id-attr)
             (some-> order-by k->attr get-column))
       :wrap-targets (fn [targets] {attr-k (mapv (fn [t] {target-k t}) targets)})
       :empty-result {attr-k []}  ;; Default for IDs with no results
       :outputs [{attr-k [target-k]}]
       :op-name (make-to-many-resolver-name attr-k)
       :id-attr-k id-attr-k
       :schema (::attr/schema (k->attr id-attr-k))
       :transform transform})))

(defn build-to-one-resolver-config
  "Build all configuration needed for a reverse to-one resolver at generation time.
   Returns a map with :sql, :ref-column, :transform-row, :outputs, :op-name, :schema, :attr-k"
  [{::attr/keys [schema]
    ::pco/keys [transform]
    attr-k ::attr/qualified-key
    target-k ::attr/target}
   id-attr-k
   id-attr->attributes
   k->attr
   ref]
  (let [ref-attr (k->attr ref)
        target-attr (k->attr target-k)
        column->outputs (get-column->outputs target-k id-attr->attributes k->attr true)
        column-mapping (get-column-mapping column->outputs)
        column->attr (get-column->attr target-attr id-attr->attributes k->attr)
        pg2-column-config (build-pg2-column-config column-mapping column->attr)
        outputs (into [] cat (vals column->outputs))
        columns (keys column->outputs)
        id-attr (k->attr id-attr-k)]
    {:sql (build-select-by-ids-sql columns (get-table target-attr) (get-column ref-attr) (get-pg-array-type id-attr))
     :ref-column (get-column ref-attr)
     :transform-row (pg2/compile-pg2-row-transformer pg2-column-config)
     :outputs outputs
     :full-outputs (conj outputs {attr-k outputs})
     :op-name (make-to-one-resolver-name target-k id-attr-k)
     :id-attr-k id-attr-k
     :attr-k attr-k
     :schema schema
     :transform transform}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Resolver Factories (thin wrappers that create Pathom resolvers)

(defn id-resolver
  "Generate a batch resolver for fetching entities by their ID attribute."
  [opts]
  (let [{:keys [sql id-column id-attr-k transform-row outputs op-name schema transform]}
        (build-id-resolver-config opts)]
    (log/debug "Generating resolver for id key" id-attr-k "to resolve" outputs)
    (pco/resolver
     op-name
     (cond->
      {::pco/output outputs
       ::pco/batch? true
       ::pco/input [id-attr-k]
       ::pco/resolve
       (fn [env input]
         (let [ids (mapv id-attr-k input)]
           (when (seq ids)
             (pg2/timer
              "id-resolver"
              (let [pool (get-pool env schema)
                    rows (pg2/pg2-query-prepared! pool sql ids)
                    results-by-id (index-rows-by-key rows #(get % id-column) transform-row)]
                (auth/redact env (order-results-by-ids results-by-id ids)))
              {:op-name op-name}))))}
       transform (assoc ::pco/transform transform)))))

(defn to-many-resolver
  "Generate a batch resolver for to-many (reverse) references."
  [attribute id-attr-k k->attr]
  (when-let [{:keys [sql wrap-targets empty-result outputs op-name id-attr-k schema transform]}
             (build-to-many-resolver-config attribute id-attr-k k->attr)]
    (log/debug "Building Pathom3 resolver" op-name "for" (::attr/qualified-key attribute) "by" id-attr-k)
    (pco/resolver
     op-name
     (cond->
      {::pco/output outputs
       ::pco/batch? true
       ::pco/input [id-attr-k]
       ::pco/resolve
       (fn [env input]
         (let [ids (mapv id-attr-k input)]
           (when (seq ids)
             (pg2/timer
              "to-many-resolver"
              (let [pool (get-pool env schema)
                    rows (pg2/pg2-query-prepared! pool sql ids)
                    results-by-id (index-rows-by-key rows :k #(wrap-targets (:v %)))
                    ;; Return empty-result for IDs with no results (instead of nil)
                    results (mapv #(get results-by-id % empty-result) ids)]
                (auth/redact env results))
              {:op-name op-name}))))}
       transform (assoc ::pco/transform transform)))))

(defn to-one-resolver
  "Generate a resolver for to-one references.
   Two cases:
   1. Forward ref (no ::rad.pg2/fk-attr): Source table has FK column, use alias resolver
   2. Reverse ref (has ::rad.pg2/fk-attr): Target table has FK, generate batch resolver"
  [{::rad.pg2/keys [fk-attr] attr-k ::attr/qualified-key target-k ::attr/target :as attribute}
   id-attr-k
   id-attr->attributes
   k->attr]
  (if-not fk-attr
    ;; Forward ref: alias to reuse the id-resolver of the target entity
    (pbir/alias-resolver (to-one-keyword attr-k) target-k)
    ;; Reverse ref: generate batch resolver
    (let [{:keys [sql ref-column transform-row full-outputs op-name id-attr-k attr-k schema transform]}
          (build-to-one-resolver-config attribute id-attr-k id-attr->attributes k->attr fk-attr)]
      (pco/resolver
       op-name
       (cond->
        {::pco/output full-outputs
         ::pco/batch? true
         ::pco/input [id-attr-k]
         ::pco/resolve
         (fn [env input]
           (let [ids (mapv id-attr-k input)]
             (when (seq ids)
               (pg2/timer
                "to-one-resolver"
                (let [pool (get-pool env schema)
                      rows (pg2/pg2-query-prepared! pool sql ids)
                      results-by-id (index-rows-by-key rows #(get % ref-column) transform-row)]
                  ;; Self-referential wrapper: assoc the entity under its ref attribute key
                  ;; so Pathom can traverse parent→child via the ref (e.g., {:user/address {...}})
                  (auth/redact env (order-results-by-ids-with-wrapper
                                    results-by-id ids
                                    #(assoc % attr-k %))))
                {:op-name op-name}))))}
         transform (assoc ::pco/transform transform))))))

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
                    :one (to-one-resolver attribute id-attr-k id-attr->attributes k->attr)
                    :many (to-many-resolver attribute id-attr-k k->attr)))))
             (remove nil?))]
    (vec (concat id-resolvers target-resolvers))))
