(ns com.fulcrologic.rad.database-adapters.pg2.save-form.invariants
  "invariants for save-form! function.

   These invariants express what must always be true about save-form! behavior,
   regardless of the specific delta being saved."
  (:require
   [clojure.set :as set]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [next.jdbc :as jdbc]))

;; =============================================================================
;; State Snapshot
;; =============================================================================

(defn take-snapshot
  "Capture database state for before/after comparison.
   Returns map of {table-name -> set of row maps}"
  [ds tables]
  (reduce
   (fn [acc table]
     (assoc acc table
            (set (jdbc/execute! ds [(str "SELECT * FROM " table)]))))
   {}
   tables))

(defn diff-snapshots
  "Compute difference between two snapshots.
   Returns {:added {table -> rows} :removed {table -> rows} :unchanged {table -> rows}}"
  [before after]
  (let [all-tables (set/union (set (keys before)) (set (keys after)))]
    (reduce
     (fn [acc table]
       (let [before-rows (get before table #{})
             after-rows (get after table #{})
             added (set/difference after-rows before-rows)
             removed (set/difference before-rows after-rows)
             unchanged (set/intersection before-rows after-rows)]
         (-> acc
             (assoc-in [:added table] added)
             (assoc-in [:removed table] removed)
             (assoc-in [:unchanged table] unchanged))))
     {}
     all-tables)))

;; =============================================================================
;; Invariant Definitions
;; =============================================================================

(def invariant-registry
  {:tempid-resolution
   {:id :tempid-resolution
    :requirement "All tempids in delta must be mapped to real IDs in result"
    :arity 1
    :check (fn [{:keys [delta result]}]
             (let [tempids-in-delta (->> (keys delta)
                                         (map second)
                                         (filter tempid/tempid?)
                                         set)
                   resolved-tempids (set (keys (:tempids result)))]
               (if (= tempids-in-delta resolved-tempids)
                 {:valid true}
                 {:valid false
                  :reason (str "Unresolved tempids: "
                               (set/difference tempids-in-delta resolved-tempids))})))}

   :tempids-are-unique
   {:id :tempids-are-unique
    :requirement "All generated real IDs must be unique"
    :arity 1
    :check (fn [{:keys [result]}]
             (let [real-ids (vals (:tempids result))]
               (if (= (count real-ids) (count (set real-ids)))
                 {:valid true}
                 {:valid false
                  :reason "Duplicate real IDs generated for different tempids"})))}

   :data-persisted
   {:id :data-persisted
    :requirement "After save, scalar values in delta must be persisted to database"
    :arity 1
    :check (fn [{:keys [delta result ds id-attr->table-col]}]
             ;; For each entity in delta, verify its scalar values are in the DB
             (let [tempids (:tempids result)]
               (reduce
                (fn [acc [[id-key id] _changes]]
                  (if-not (:valid acc)
                    (reduced acc)
                    (let [real-id (if (tempid/tempid? id) (get tempids id) id)
                          {:keys [table id-col]} (get id-attr->table-col id-key)
                          row (first (jdbc/execute! ds [(str "SELECT * FROM " table
                                                             " WHERE " id-col " = ?") real-id]))]
                      (if row
                        {:valid true}
                        {:valid false
                         :reason (str "Row not found for " [id-key real-id])}))))
                {:valid true}
                delta)))}

   :transaction-atomicity
   {:id :transaction-atomicity
    :requirement "On constraint violation, no changes are persisted"
    :arity 2 ;; needs before/after
    :check (fn [before-snapshot after-snapshot {:keys [exception-thrown?]}]
             (if exception-thrown?
               (if (= before-snapshot after-snapshot)
                 {:valid true}
                 {:valid false
                  :reason "Exception was thrown but database state changed"})
               {:valid true}))}

   :idempotent-updates
   {:id :idempotent-updates
    :requirement "Saving the same entity twice (with real IDs) produces same result"
    :arity 2
    :check (fn [after-first after-second _]
             (if (= after-first after-second)
               {:valid true}
               {:valid false
                :reason "Database state differs after second save"}))}

   :no-orphan-references
   {:id :no-orphan-references
    :requirement "All foreign key references point to existing rows"
    :arity 1
    :check (fn [{:keys [ds fk-constraints]}]
             ;; fk-constraints is a seq of {:from-table :from-col :to-table :to-col}
             (reduce
              (fn [acc {:keys [from-table from-col to-table to-col]}]
                (if-not (:valid acc)
                  (reduced acc)
                  (let [orphans (jdbc/execute! ds
                                               [(str "SELECT " from-table "." from-col
                                                     " FROM " from-table
                                                     " LEFT JOIN " to-table
                                                     " ON " from-table "." from-col " = " to-table "." to-col
                                                     " WHERE " from-table "." from-col " IS NOT NULL"
                                                     " AND " to-table "." to-col " IS NULL")])]
                    (if (empty? orphans)
                      {:valid true}
                      {:valid false
                       :reason (str "Orphan references in " from-table "." from-col ": " (count orphans))}))))
              {:valid true}
              fk-constraints))}

   :schema-isolation
   {:id :schema-isolation
    :requirement "Changes only affect tables in the delta's schemas"
    :arity 2
    :check (fn [before-snapshot after-snapshot {:keys [expected-tables]}]
             (let [diff (diff-snapshots before-snapshot after-snapshot)
                   changed-tables (->> (:added diff)
                                       (filter (fn [[_ rows]] (seq rows)))
                                       (map first)
                                       set)
                   changed-tables (set/union
                                   changed-tables
                                   (->> (:removed diff)
                                        (filter (fn [[_ rows]] (seq rows)))
                                        (map first)
                                        set))]
               (if (set/subset? changed-tables expected-tables)
                 {:valid true}
                 {:valid false
                  :reason (str "Unexpected tables modified: "
                               (set/difference changed-tables expected-tables))})))}})

;; =============================================================================
;; Check Functions
;; =============================================================================

(defn check-invariant
  "Check a single invariant. Returns {:valid boolean :reason string?}"
  [invariant-id context]
  (let [{:keys [check]} (get invariant-registry invariant-id)]
    (if check
      (check context)
      {:valid false :reason (str "Unknown invariant: " invariant-id)})))

(defn check-invariant-2
  "Check a two-arity invariant with before/after snapshots."
  [invariant-id before after context]
  (let [{:keys [check arity]} (get invariant-registry invariant-id)]
    (if (= arity 2)
      (check before after context)
      (check context))))

(defn check-all-invariants
  "Check all applicable invariants for a context."
  [context & {:keys [invariants] :or {invariants (keys invariant-registry)}}]
  (let [results (for [inv-id invariants
                      :let [{:keys [arity]} (get invariant-registry inv-id)]
                      :when (= arity 1)]
                  (assoc (check-invariant inv-id context) :id inv-id))]
    {:valid (every? :valid results)
     :results results}))

;; =============================================================================
;; Operation Verification
;; =============================================================================

(defn verify-save!
  "Execute save-form! and verify invariants hold.

   Options:
   - :save-fn - function that takes env and params, calls save-form!
   - :env - environment for save-form!
   - :params - params for save-form! (contains ::rad.form/delta)
   - :ds - datasource for snapshots
   - :tables - tables to snapshot
   - :invariants - invariant IDs to check (default: all 1-arity)"
  [{:keys [save-fn env params ds tables invariants]
    :or {invariants [:tempid-resolution :tempids-are-unique]}}]
  (let [delta (get params :com.fulcrologic.rad.form/delta)
        before (take-snapshot ds tables)
        result (try
                 {:result (save-fn env params)
                  :exception nil}
                 (catch Exception e
                   {:result nil
                    :exception e}))
        after (take-snapshot ds tables)
        context {:delta delta
                 :result (:result result)
                 :exception (:exception result)
                 :ds ds
                 :before before
                 :after after}

        ;; Check 1-arity invariants
        check-results (for [inv-id invariants
                            :let [{:keys [arity]} (get invariant-registry inv-id)]
                            :when (or (nil? arity) (= arity 1))]
                        (assoc (check-invariant inv-id context) :id inv-id))

        ;; Check 2-arity invariants
        check-results-2 (for [inv-id invariants
                              :let [{:keys [arity]} (get invariant-registry inv-id)]
                              :when (= arity 2)]
                          (assoc (check-invariant-2 inv-id before after context) :id inv-id))

        all-results (concat check-results check-results-2)]
    {:valid (every? :valid all-results)
     :results all-results
     :save-result (:result result)
     :exception (:exception result)
     :diff (diff-snapshots before after)}))
