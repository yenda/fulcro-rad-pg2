(ns com.fulcrologic.rad.database-adapters.sql.resolvers
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.set :as set]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.malli-transform :as mt]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
   [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [diehard.core :as dh]
   [edn-query-language.core :as eql]
   [honey.sql :as sql]
   [next.jdbc :as jdbc]
   [pg.core :as pg]
   [pg.pool :as pg.pool]
   [taoensso.timbre :as log])
  (:import
   (org.postgresql.util PSQLException)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Driver Abstraction
;;
;; These functions allow transparent use of either HikariCP/next.jdbc or pg2

(defn- extract-pool
  "Extract the actual pool/datasource from a pool wrapper.
   Returns [driver pool] tuple."
  [pool-or-wrapper]
  (if (map? pool-or-wrapper)
    [(:driver pool-or-wrapper) (:pool pool-or-wrapper)]
    [:hikaricp pool-or-wrapper]))

(defn- execute-sql!
  "Execute SQL statement with driver dispatch.
   For reads, returns vector of maps. For writes, returns affected count."
  [pool-or-wrapper sql-vec]
  (let [[driver pool] (extract-pool pool-or-wrapper)]
    (case driver
      :pg2 (pg2/pg2-query! pool sql-vec)
      ;; Default: next.jdbc
      (jdbc/execute! pool sql-vec))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Writes

(defn resolve-tempid-in-value [tempids v]
  (cond
    (tempid/tempid? v) (get tempids v v)
    (eql/ident? v) (let [[k id] v]
                     (if (tempid/tempid? id)
                       [k (get tempids id id)]
                       v))
    :else v))

(def keys-in-delta
  (fn keys-in-delta [delta]
    (let [id-keys (into #{}
                        (map first)
                        (keys delta))
          all-keys (into id-keys
                         (mapcat keys)
                         (vals delta))]
      all-keys)))

(defn schemas-for-delta [{::attr/keys [key->attribute]} delta]
  (let [all-keys (keys-in-delta delta)
        schemas (into #{}
                      (keep #(-> % key->attribute ::attr/schema))
                      all-keys)]
    schemas))

(defn- generate-tempids [pool-or-wrapper key->attribute delta]
  (reduce
   (fn [result [table id]]
     (if (tempid/tempid? id)
       (let [{::attr/keys [type] :as id-attr} (key->attribute table)
             real-id (if (#{:int :long} type)
                       (:id (first (execute-sql! pool-or-wrapper
                                                 [(format "SELECT NEXTVAL('%s') AS id"
                                                          (sql.schema/sequence-name id-attr))])))
                       (ids/new-uuid))]
         (assoc result id real-id))
       result))
   {}
   (keys delta)))

(defn- table-local-attr
  "Returns an attribute or nil if it isn't stored on the semantic table of the attribute."
  [k->a schema-to-save k]
  (let [{::attr/keys [type cardinality schema] :as attr} (k->a k)]
    (when (= schema schema-to-save)
      (cond
        (and (= cardinality :many) (= :ref type))
        nil

        (= cardinality :one)
        (if (::rad.sql/ref attr)
          nil
          attr)

        :else
        attr))))

(defn form->sql-value [{::attr/keys [type cardinality]
                        ::rad.sql/keys [form->sql-value]} form-value]
  (cond
    ;; References: extract ID from ident
    (and (= :ref type) (not= :many cardinality) (eql/ident? form-value))
    (second form-value)

    ;; Custom transformer takes precedence
    form->sql-value
    (form->sql-value form-value)

    ;; Use Malli-based transformation for all types
    :else
    (mt/encode-for-sql type form-value)))

(defn sql->model-value
  "Transform SQL value to Clojure model value.

   This is the read-path counterpart to form->sql-value. It applies
   type-specific transformations to convert database values back to
   their Clojure representations.

   Arguments:
     attr      - The attribute map containing ::attr/type and optional ::rad.sql/sql->model-value
     sql-value - The value retrieved from the database

   Returns:
     The transformed Clojure value."
  [{::attr/keys [type]
    ::rad.sql/keys [sql->model-value]} sql-value]
  (cond
    ;; Custom transformer takes precedence
    sql->model-value
    (sql->model-value sql-value)

    ;; Use Malli-based transformation
    :else
    (mt/decode-from-sql type sql-value)))

(defn scalar-insert
  [{::attr/keys [key->attribute] :as env} schema-to-save tempids [table id :as ident] diff]
  (when (tempid/tempid? (log/spy :trace id))
    (let [{::attr/keys [type schema] :as id-attr} (key->attribute table)]
      (if (= schema schema-to-save)
        (let [table-kw (keyword (sql.schema/table-name key->attribute id-attr))
              real-id (get tempids id id)
              scalar-attrs (keep
                            (fn [k]
                              (table-local-attr key->attribute schema-to-save k))
                            (keys diff))
              new-val (fn [{::attr/keys [qualified-key schema] :as attr}]
                        (when (= schema schema-to-save)
                          (let [v (get-in diff [qualified-key :after])
                                v (resolve-tempid-in-value tempids v)]
                            (form->sql-value attr v))))
              values (reduce (fn [acc attr]
                               (let [v (new-val attr)]
                                 (if (nil? v)
                                   acc
                                   (assoc acc (keyword (sql.schema/column-name attr)) v))))
                             {(keyword (sql.schema/column-name id-attr)) real-id}
                             scalar-attrs)]
          (sql/format {:insert-into table-kw
                       :values [values]
                       :returning [:*]}))
        (log/debug "Schemas do not match. Not updating" ident)))))

(defn delta->scalar-inserts [{::attr/keys [key->attribute]
                              ::rad.sql/keys [connection-pools]
                              :as env} schema delta]
  (let [ds (get connection-pools schema)
        tempids (log/spy :trace (generate-tempids ds key->attribute delta))
        stmts (keep (fn [[ident diff]] (scalar-insert env schema tempids ident diff)) delta)]
    {:tempids tempids
     :insert-scalars stmts}))

(defn scalar-update
  [{::keys [tempids]
    ::attr/keys [key->attribute] :as env} tempids schema-to-save [table id :as ident] {:keys [after] :as diff}]
  (when-not (tempid/tempid? id)
    (let [{::attr/keys [type schema] :as id-attr} (key->attribute table)]
      (when (= schema-to-save schema)
        (let [table-kw (keyword (sql.schema/table-name key->attribute id-attr))
              id-col-kw (keyword (sql.schema/column-name id-attr))]
          (if (:delete diff)
            (sql/format {:delete-from table-kw
                         :where [:= id-col-kw id]})
            (let [scalar-attrs (keep
                                (fn [k] (table-local-attr key->attribute schema-to-save k))
                                (keys diff))
                  old-val (fn [{::attr/keys [qualified-key] :as attr}]
                            (some->> (get-in diff [qualified-key :before])
                                     (form->sql-value attr)))
                  new-val (fn [{::attr/keys [qualified-key schema] :as attr}]
                            (when (= schema schema-to-save)
                              (let [v (get-in diff [qualified-key :after])
                                    v (resolve-tempid-in-value tempids v)]
                                (form->sql-value attr v))))
                  values (reduce
                          (fn [result attr]
                            (let [new (log/spy :trace (new-val attr))
                                  col-name (keyword (sql.schema/column-name attr))
                                  old (old-val attr)]
                              (cond
                                (= new :delete)
                                (reduced :delete)
                                (and old (nil? new))
                                (assoc result col-name nil)

                                (not (nil? new))
                                (assoc result col-name new)

                                :else
                                result)))
                          {}
                          scalar-attrs)]
              (if (= :delete values)
                (sql/format {:delete-from table-kw
                             :where [:= id-col-kw id]})
                (when (seq values)
                  (sql/format {:update table-kw
                               :set values
                               :where [:= id-col-kw id]}))))))))))

(defn delta->scalar-updates [env tempids schema delta]
  (vec (keep (fn [[ident diff]] (scalar-update env tempids schema ident diff)) delta)))

(defn process-attributes [key->attribute delta]
  (reduce (fn [acc [ident attributes]]
            (reduce (fn [acc [attr-k value]]
                      (let [{::attr/keys [type cardinality]
                             ::rad.sql/keys [ref delete-referent?] :as attribute}
                            (key->attribute attr-k)
                            {:keys [before after]} value]
                        (if (and ref
                                 (not (= before after nil)))
                          (case cardinality
                            :one
                            (if (:after value)
                              (assoc-in acc [(:after value) ref] {:after ident})
                              ;; if the relation has been removed and the `delete-referent?`
                              ;; is true the referenced row will be deleted
                              (assoc-in acc [(:before value) ref] {:before ident
                                                                   :after (if delete-referent?
                                                                            :delete
                                                                            nil)}))
                            :many
                            (let [after-set (into #{} after)
                                  before-set (into #{} before)]

                              ;; check if a given ref-ident has been added or removed
                              ;; from the to-many relationship
                              (reduce (fn [acc ref-ident]
                                        (cond
                                          (and (after-set ref-ident)
                                               (not (before-set ref-ident)))
                                          (assoc-in acc [ref-ident ref] {:after ident})

                                          (and (before-set ref-ident)
                                               (not (after-set ref-ident)))
                                          (assoc-in acc [ref-ident ref]
                                                    {:before ident
                                                     :after (get-in acc
                                                                    [ref-ident ref :after]
                                                                    ;; if it has been removed and the `delete-referent?`
                                                                    ;; is true the referenced row will be deleted
                                                                    (if delete-referent?
                                                                      :delete
                                                                      nil))})

                                          (and (after-set ref-ident)
                                               (before-set ref-ident))
                                          acc))
                                      acc
                                      (set/union after-set before-set))))
                          acc)))
                    acc
                    attributes))
          delta
          delta))

(defn error-condition
  "Return the error condition from an error by looking up the error code.
  Add new error code when needed from here:
  https://www.postgresql.org/docs/12/errcodes-appendix.html"
  [^PSQLException e]
  (case (.getSQLState e)
    "08003" ::connection-does-not-exist
    "22001" ::string-data-too-long
    "22021" ::invalid-encoding
    "22P02" ::invalid-text-representation
    "23502" ::not-null-violation
    "23505" ::unique-violation
    "23514" ::check-violation
    "40001" ::serialization-failure
    "57014" ::timeout
    ::unknown))

(defn- wrap-save-error
  "Wrap a PSQLException in an ex-info with structured error data.
   Preserves the original exception as the cause."
  [^PSQLException e]
  (let [condition (error-condition e)]
    (ex-info (str "save-form! failed: " (name condition))
             {:type ::save-error
              :cause condition
              :sql-state (.getSQLState e)
              :message (.getMessage e)}
             e)))

(defn- pg2-execute!
  "Execute SQL using pg2 connection (for use within transaction)."
  [conn sql-vec]
  (let [[sql & params] sql-vec
        pg2-sql (pg2/convert-params sql)]
    (pg/execute conn pg2-sql {:params (vec params)})))

(defn save-form!
  "Does all the necessary operations to persist mutations from the
  form delta into the appropriate tables in the appropriate databases.

   Throws ex-info with :type ::save-error for database constraint violations.
   The ex-data includes:
     :type      - ::save-error
     :cause     - keyword like ::string-data-too-long, ::invalid-encoding, etc.
     :sql-state - PostgreSQL error code
     :message   - Original error message
   The original PSQLException is preserved as the cause."
  [{::attr/keys [key->attribute]
    ::rad.sql/keys [connection-pools adapters default-adapter]
    :as env} {::rad.form/keys [delta] :as d}]
  (try
    (let [delta-before (with-out-str (pprint delta))
          schemas (schemas-for-delta env delta)
          delta (process-attributes key->attribute delta)
          result (atom {:tempids {}})]
      (log/debug "Saving form across " schemas
                 {:delta delta-before
                  :processed-delta (with-out-str (pprint delta))})
      ;; TASK: Transaction should be opened on all databases at once, so that they all succeed or fail
      (doseq [schema (keys connection-pools)]
        (let [adapter (get adapters schema default-adapter)
              pool-wrapper (get connection-pools schema)
              [driver pool] (extract-pool pool-wrapper)
              {:keys [tempids insert-scalars]} (log/spy :trace (delta->scalar-inserts env schema delta))
              update-scalars (log/spy :trace (delta->scalar-updates env tempids schema delta))
              steps (concat update-scalars insert-scalars)]
          (dh/with-retry
            {:retry-if (fn [_return-value exception-thrown]
                         (if (and exception-thrown
                                  (instance? PSQLException exception-thrown)
                                  (= ::serialization-failure (error-condition exception-thrown)))
                           true
                           false))
             :max-retries 4
             :backoff-ms [100 200 2.0]}
            (case driver
              :pg2
              (pg.pool/with-conn [conn pool]
                (pg/with-transaction [tx conn {:isolation-level :serializable}]
                  (when adapter
                    (vendor/relax-constraints! adapter tx pg2-execute!))
                  (doseq [stmt-with-params steps]
                    (log/debug "save-form!" {:stmt-with-params stmt-with-params})
                    (pg2-execute! tx stmt-with-params))))

              ;; Default: HikariCP/next.jdbc
              (jdbc/with-transaction [tx pool {:isolation :serializable}]
                (when adapter
                  (vendor/relax-constraints! adapter tx))
                (doseq [stmt-with-params steps]
                  (log/debug "save-form!" {:stmt-with-params stmt-with-params})
                  (jdbc/execute! tx stmt-with-params)))))
          (swap! result update :tempids merge tempids)))
      @result)
    (catch PSQLException e
      (throw (wrap-save-error e)))))
