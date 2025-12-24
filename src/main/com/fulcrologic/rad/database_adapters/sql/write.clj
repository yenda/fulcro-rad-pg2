(ns com.fulcrologic.rad.database-adapters.sql.write
  "Save/write logic for pg2 PostgreSQL driver.

   Architecture:
   - Pure functions compute SQL plans from deltas
   - Side effects (DB calls) are pushed to the edges
   - save-form! orchestrates: plan → allocate IDs → generate SQL → execute"
  (:require
   [clojure.set :as set]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [diehard.core :as dh]
   [edn-query-language.core :as eql]
   [honey.sql :as sql]
   [pg.core :as pg]
   [pg.pool :as pg.pool]
   [taoensso.timbre :as log])
  (:import
   (org.postgresql.util PSQLException)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pure: Value Transformation

(defn resolve-tempid-in-value
  "Replace tempid with real ID in a value (scalar or ident)."
  [tempids v]
  (cond
    (tempid/tempid? v)
    (get tempids v v)

    (eql/ident? v)
    (let [[k id] v]
      (if (tempid/tempid? id)
        [k (get tempids id id)]
        v))

    :else v))

(defn form->sql-value
  "Transform form value to SQL value based on attribute type."
  [{::attr/keys [type cardinality]
    ::rad.sql/keys [form->sql-value]} form-value]
  (cond
    (and (= :ref type) (not= :many cardinality) (eql/ident? form-value))
    (second form-value)

    form->sql-value
    (form->sql-value form-value)

    :else
    (pg2/encode-for-sql type form-value)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pure: Delta Analysis

(defn keys-in-delta
  "Extract all attribute keys referenced in a delta."
  [delta]
  (let [id-keys (into #{} (map first) (keys delta))
        all-keys (into id-keys (mapcat keys) (vals delta))]
    all-keys))

(defn schemas-for-delta
  "Determine which schemas are affected by a delta."
  [{::attr/keys [key->attribute]} delta]
  (into #{}
        (keep #(-> % key->attribute ::attr/schema))
        (keys-in-delta delta)))

(defn process-ref-attributes
  "Process reference attributes to generate reverse updates.
   Pure function - transforms delta to include reference column updates."
  [key->attribute delta]
  (reduce
   (fn [acc [ident attributes]]
     (reduce
      (fn [acc [attr-k {:keys [before after]}]]
        (let [{::attr/keys [type cardinality]
               ::rad.sql/keys [ref delete-referent?]} (key->attribute attr-k)]
          (if (and ref (not (= before after nil)))
            (case cardinality
              :one
              (if after
                (assoc-in acc [after ref] {:after ident})
                (assoc-in acc [before ref] {:before ident
                                            :after (when delete-referent? :delete)}))
              :many
              (let [after-set (set after)
                    before-set (set before)]
                (reduce
                 (fn [acc ref-ident]
                   (cond
                     (and (after-set ref-ident) (not (before-set ref-ident)))
                     (assoc-in acc [ref-ident ref] {:after ident})

                     (and (before-set ref-ident) (not (after-set ref-ident)))
                     (assoc-in acc [ref-ident ref]
                               {:before ident
                                :after (get-in acc [ref-ident ref :after]
                                               (when delete-referent? :delete))})

                     :else acc))
                 acc
                 (set/union after-set before-set))))
            acc)))
      acc
      attributes))
   delta
   delta))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pure: Tempid Planning

(defn plan-tempids
  "Analyze delta to determine how to allocate each tempid.
   Returns {:sequence-ids {tempid {:attr id-attr :sequence seq-name}}
            :uuid-ids [tempid ...]}"
  [key->attribute delta]
  (reduce
   (fn [plan [table id]]
     (if (tempid/tempid? id)
       (let [{::attr/keys [type] :as id-attr} (key->attribute table)]
         (if (#{:int :long} type)
           (update plan :sequence-ids assoc id
                   {:attr id-attr
                    :sequence (sql.schema/sequence-name id-attr)})
           (update plan :uuid-ids conj id)))
       plan))
   {:sequence-ids {}
    :uuid-ids []}
   (keys delta)))

(defn resolve-uuid-tempids
  "Generate UUIDs for tempids that don't need sequences. Pure function."
  [uuid-ids]
  (into {} (map (fn [tid] [tid (ids/new-uuid)])) uuid-ids))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pure: SQL Generation

(defn- table-local-attr
  "Returns attribute if it's stored on the table for this schema, nil otherwise."
  [k->a target-schema k]
  (let [{::attr/keys [type cardinality schema] :as attr} (k->a k)]
    (when (= schema target-schema)
      (cond
        (and (= cardinality :many) (= :ref type)) nil
        (= cardinality :one) (when-not (::rad.sql/ref attr) attr)
        :else attr))))

(defn generate-insert
  "Generate INSERT statement for a new entity. Pure function."
  [key->attribute target-schema tempids [table id] diff]
  (when (tempid/tempid? id)
    (let [{::attr/keys [schema] :as id-attr} (key->attribute table)]
      (when (= schema target-schema)
        (let [table-kw (keyword (sql.schema/table-name key->attribute id-attr))
              real-id (get tempids id id)
              scalar-attrs (keep #(table-local-attr key->attribute target-schema %) (keys diff))
              values (reduce
                      (fn [acc attr]
                        (let [v (get-in diff [(::attr/qualified-key attr) :after])
                              v (resolve-tempid-in-value tempids v)
                              v (form->sql-value attr v)]
                          (if (nil? v)
                            acc
                            (assoc acc (keyword (sql.schema/column-name attr)) v))))
                      {(keyword (sql.schema/column-name id-attr)) real-id}
                      scalar-attrs)]
          (sql/format {:insert-into table-kw
                       :values [values]
                       :returning [:*]}))))))

(defn generate-update
  "Generate UPDATE or DELETE statement for an existing entity. Pure function."
  [key->attribute target-schema tempids [table id] diff]
  (when-not (tempid/tempid? id)
    (let [{::attr/keys [schema] :as id-attr} (key->attribute table)]
      (when (= schema target-schema)
        (let [table-kw (keyword (sql.schema/table-name key->attribute id-attr))
              id-col-kw (keyword (sql.schema/column-name id-attr))]
          (if (:delete diff)
            (sql/format {:delete-from table-kw :where [:= id-col-kw id]})
            (let [scalar-attrs (keep #(table-local-attr key->attribute target-schema %) (keys diff))
                  values (reduce
                          (fn [acc attr]
                            (let [qk (::attr/qualified-key attr)
                                  old (some->> (get-in diff [qk :before]) (form->sql-value attr))
                                  new-raw (get-in diff [qk :after])
                                  new (some->> new-raw
                                               (resolve-tempid-in-value tempids)
                                               (form->sql-value attr))
                                  col (keyword (sql.schema/column-name attr))]
                              (cond
                                (= new-raw :delete) (reduced :delete)
                                (and old (nil? new)) (assoc acc col nil)
                                (some? new) (assoc acc col new)
                                :else acc)))
                          {}
                          scalar-attrs)]
              (cond
                (= :delete values)
                (sql/format {:delete-from table-kw :where [:= id-col-kw id]})

                (seq values)
                (sql/format {:update table-kw :set values :where [:= id-col-kw id]})))))))))

(defn delta->sql-plan
  "Generate all SQL statements for a delta. Pure function.
   Returns {:inserts [...] :updates [...]} with SQL vectors."
  [key->attribute schema tempids delta]
  (let [inserts (keep (fn [[ident diff]] (generate-insert key->attribute schema tempids ident diff)) delta)
        updates (keep (fn [[ident diff]] (generate-update key->attribute schema tempids ident diff)) delta)]
    {:inserts (vec inserts)
     :updates (vec updates)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Side Effects: ID Allocation

(defn- group-tempids-by-sequence
  "Group tempids by their sequence name.
   Returns {sequence-name [tempid1 tempid2 ...]}."
  [sequence-ids]
  (reduce-kv
   (fn [acc tempid {:keys [sequence]}]
     (update acc sequence (fnil conj []) tempid))
   {}
   sequence-ids))

(defn- allocate-batch-from-sequence!
  "Allocate N IDs from a single sequence in one query.
   Returns vector of allocated IDs in order."
  [conn sequence-name n]
  (if (= n 1)
    ;; Single ID - simple query
    [(:id (first (pg/execute conn (format "SELECT NEXTVAL('%s') AS id" sequence-name))))]
    ;; Multiple IDs - batch query using generate_series
    (let [sql (format "SELECT NEXTVAL('%s') AS id FROM generate_series(1, %d)" sequence-name n)
          results (pg/execute conn sql)]
      (mapv :id results))))

(defn allocate-sequence-ids!
  "Allocate IDs from database sequences. Side effect at the edge.

   Optimization: Groups tempids by sequence name and allocates each batch
   in a single query, reducing N individual NEXTVAL calls to M queries
   where M is the number of distinct sequences.

   Returns map of tempid -> real-id."
  [pool sequence-ids]
  (if (empty? sequence-ids)
    {}
    (pg.pool/with-conn [conn pool]
      (let [by-sequence (group-tempids-by-sequence sequence-ids)]
        (reduce-kv
         (fn [acc sequence-name tempids]
           (let [ids (allocate-batch-from-sequence! conn sequence-name (count tempids))]
             ;; Zip tempids with allocated IDs
             (into acc (map vector tempids ids))))
         {}
         by-sequence)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Side Effects: SQL Execution

(defn- execute-statement!
  "Execute a single SQL statement. Side effect."
  [conn sql-vec]
  (let [[sql & params] sql-vec
        pg2-sql (pg2/convert-params sql)]
    (pg/execute conn pg2-sql {:params (vec params)})))

(defn execute-plan!
  "Execute a SQL plan within a transaction. Side effect at the edge.
   Executes updates before inserts (for proper FK ordering with deferred constraints)."
  [pool {:keys [updates inserts]}]
  (let [statements (concat updates inserts)]
    (when (seq statements)
      (pg.pool/with-conn [conn pool]
        (pg/with-transaction [tx conn {:isolation-level :serializable}]
          (pg2/relax-constraints! tx)
          (doseq [stmt statements]
            (log/debug "execute-plan!" {:stmt stmt})
            (execute-statement! tx stmt)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Error Handling

(defn error-condition
  "Map PostgreSQL error code to semantic condition."
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
  "Wrap PSQLException in ex-info with structured data."
  [^PSQLException e]
  (let [condition (error-condition e)]
    (ex-info (str "save-form! failed: " (name condition))
             {:type ::save-error
              :cause condition
              :sql-state (.getSQLState e)
              :message (.getMessage e)}
             e)))

(defn- retryable?
  "Check if an exception is retryable (serialization failure)."
  [_ e]
  (and (instance? PSQLException e)
       (= ::serialization-failure (error-condition e))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Main API

(defn save-form!
  "Persist form mutations to the database.

   Architecture:
   1. PURE: Process delta (expand ref attributes)
   2. PURE: Plan tempid allocation (sequences vs UUIDs)
   3. EFFECT: Allocate sequence IDs from database
   4. PURE: Resolve all tempids (sequences + UUIDs)
   5. PURE: Generate SQL plan from delta + resolved tempids
   6. EFFECT: Execute SQL plan in transaction

   Returns {:tempids {tempid -> real-id}}"
  [{::attr/keys [key->attribute]
    ::rad.sql/keys [connection-pools]
    :as env}
   {::rad.form/keys [delta]}]
  (try
    ;; PURE: Process delta to expand reference attributes
    (let [delta (process-ref-attributes key->attribute delta)]
      (log/debug "Saving form" {:schemas (schemas-for-delta env delta)})

      ;; Process each schema
      (reduce-kv
       (fn [result schema pool]
         ;; PURE: Plan how to allocate tempids
         (let [tempid-plan (plan-tempids key->attribute delta)

               ;; EFFECT: Allocate sequence IDs (single DB round-trip)
               seq-tempids (dh/with-retry
                             {:retry-if retryable?
                              :max-retries 4
                              :backoff-ms [100 200 2.0]}
                             (allocate-sequence-ids! pool (:sequence-ids tempid-plan)))

               ;; PURE: Resolve UUID tempids
               uuid-tempids (resolve-uuid-tempids (:uuid-ids tempid-plan))

               ;; PURE: Combine all resolved tempids
               tempids (merge seq-tempids uuid-tempids)

               ;; PURE: Generate SQL plan
               sql-plan (delta->sql-plan key->attribute schema tempids delta)]

           ;; EFFECT: Execute plan in transaction (with retry)
           (dh/with-retry
             {:retry-if retryable?
              :max-retries 4
              :backoff-ms [100 200 2.0]}
             (execute-plan! pool sql-plan))

           ;; Accumulate tempids
           (update result :tempids merge tempids)))
       {:tempids {}}
       connection-pools))

    (catch PSQLException e
      (throw (wrap-save-error e)))))
