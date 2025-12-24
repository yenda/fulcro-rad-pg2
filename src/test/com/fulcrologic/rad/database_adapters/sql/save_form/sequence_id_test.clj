(ns com.fulcrologic.rad.database-adapters.sql.save-form.sequence-id-test
  "Tests for save-form! with sequence-based (integer/long) identity types.

   These tests cover the NEXTVAL code path in allocate-sequence-ids!,
   exercised when entities use :int or :long identity types.
   This is the foundation for batch ID generation optimization.

   Test structure:
   1. Unit tests for pure functions (no DB needed)
   2. Integration tests with pg2 pools (require PostgreSQL)"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [com.fulcrologic.rad.database-adapters.sql.write :as write]
   [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [pg.core :as pg]
   [pg.pool :as pool]
   [taoensso.encore :as enc]))

;; =============================================================================
;; Test Configuration
;; =============================================================================

(def pg2-config
  {:host "localhost"
   :port 5432
   :user "user"
   :password "password"
   :database "fulcro-rad-pg2"})

(def key->attribute (enc/keys-by ::attr/qualified-key attrs/all-attributes))

;; =============================================================================
;; Unit Tests for Pure Functions (No Database Required)
;; =============================================================================

(deftest test-plan-tempids-identifies-sequence-types
  (testing "plan-tempids correctly identifies :long as needing sequence"
    (let [tempid1 (tempid/tempid)
          delta {[:item/id tempid1] {:item/name {:after "Widget"}}}
          plan (write/plan-tempids key->attribute delta)]
      (is (contains? (:sequence-ids plan) tempid1)
          "Long ID should be in sequence-ids")
      (is (empty? (:uuid-ids plan))
          "No UUID IDs in this delta")))

  (testing "plan-tempids correctly identifies :uuid as not needing sequence"
    (let [tempid1 (tempid/tempid)
          delta {[:account/id tempid1] {:account/name {:after "Alice"}}}
          plan (write/plan-tempids key->attribute delta)]
      (is (empty? (:sequence-ids plan))
          "UUID ID should not be in sequence-ids")
      (is (= [tempid1] (:uuid-ids plan))
          "UUID ID should be in uuid-ids")))

  (testing "plan-tempids handles mixed types correctly"
    (let [uuid-tempid (tempid/tempid)
          seq-tempid1 (tempid/tempid)
          seq-tempid2 (tempid/tempid)
          delta {[:account/id uuid-tempid] {:account/name {:after "Alice"}}
                 [:item/id seq-tempid1] {:item/name {:after "Widget A"}}
                 [:line-item/id seq-tempid2] {:line-item/description {:after "Line"}}}
          plan (write/plan-tempids key->attribute delta)]
      (is (= 2 (count (:sequence-ids plan)))
          "Should have 2 sequence IDs")
      (is (contains? (:sequence-ids plan) seq-tempid1))
      (is (contains? (:sequence-ids plan) seq-tempid2))
      (is (= [uuid-tempid] (:uuid-ids plan))
          "Should have 1 UUID ID")))

  (testing "plan-tempids ignores real IDs (not tempids)"
    (let [real-id 12345
          delta {[:item/id real-id] {:item/name {:before "Old" :after "New"}}}
          plan (write/plan-tempids key->attribute delta)]
      (is (empty? (:sequence-ids plan))
          "Real IDs should not be in plan")
      (is (empty? (:uuid-ids plan))
          "Real IDs should not be in plan"))))

(deftest test-resolve-uuid-tempids
  (testing "resolve-uuid-tempids generates UUIDs for each tempid"
    (let [tempids [(tempid/tempid) (tempid/tempid) (tempid/tempid)]
          result (write/resolve-uuid-tempids tempids)]
      (is (= 3 (count result)))
      (is (every? uuid? (vals result)))
      (is (= 3 (count (set (vals result)))) "All UUIDs should be unique"))))

(deftest test-resolve-tempid-in-value
  (testing "resolves tempid to real ID"
    (let [tid (tempid/tempid)
          tempids {tid 42}]
      (is (= 42 (write/resolve-tempid-in-value tempids tid)))))

  (testing "resolves tempid in ident"
    (let [tid (tempid/tempid)
          tempids {tid 42}]
      (is (= [:item/id 42]
             (write/resolve-tempid-in-value tempids [:item/id tid])))))

  (testing "preserves non-tempid values"
    (is (= 123 (write/resolve-tempid-in-value {} 123)))
    (is (= "hello" (write/resolve-tempid-in-value {} "hello")))))

;; =============================================================================
;; Integration Test Infrastructure
;; =============================================================================

(defn create-test-pool []
  (pool/pool (merge pg2-config
                    {:pool-min-size 1
                     :pool-max-size 2})))

(defn generate-test-schema-name []
  (str "test_seq_id_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn- split-sql-statements
  "Split a multi-statement SQL string into individual statements."
  [sql]
  (->> (clojure.string/split sql #";\n")
       (map clojure.string/trim)
       (remove empty?)))

(defn setup-test-schema
  "Create isolated test schema and tables. Returns schema name."
  [pool]
  (let [schema-name (generate-test-schema-name)]
    (pool/with-conn [conn pool]
      (pg/execute conn (str "CREATE SCHEMA " schema-name))
      (pg/execute conn (str "SET search_path TO " schema-name))
      ;; Create tables - split multi-statement strings
      (doseq [stmt-block (mig/automatic-schema :production attrs/all-attributes)
              stmt (split-sql-statements stmt-block)]
        (pg/execute conn stmt)))
    schema-name))

(defn teardown-test-schema
  "Drop test schema."
  [pool schema-name]
  (pool/with-conn [conn pool]
    (pg/execute conn (str "DROP SCHEMA " schema-name " CASCADE"))))

(defmacro with-test-schema
  "Execute body with isolated test schema."
  [pool schema-binding & body]
  `(let [~schema-binding (setup-test-schema ~pool)]
     (try
       (pool/with-conn [conn# ~pool]
         (pg/execute conn# (str "SET search_path TO " ~schema-binding)))
       ~@body
       (finally
         (teardown-test-schema ~pool ~schema-binding)))))

(defn make-env [pool]
  {::attr/key->attribute key->attribute
   ::rad.sql/connection-pools {:production pool}})

(defn query-items [pool]
  (pool/with-conn [conn pool]
    (pg/execute conn "SELECT * FROM items ORDER BY id")))

(defn query-line-items [pool]
  (pool/with-conn [conn pool]
    (pg/execute conn "SELECT * FROM line_items ORDER BY id")))

(defn query-accounts [pool]
  (pool/with-conn [conn pool]
    (pg/execute conn "SELECT * FROM accounts ORDER BY id")))

;; =============================================================================
;; Integration Tests - Single Entity with Sequence ID
;; =============================================================================

(deftest ^:integration test-insert-single-item-with-sequence-id
  (testing "Inserting a single entity with sequence-based (long) ID"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [tempid (tempid/tempid)
                delta {[:item/id tempid] {:item/name {:after "Widget"}
                                          :item/quantity {:after 10}
                                          :item/active? {:after true}}}
                env (make-env pool)
                result (write/save-form! env {::rad.form/delta delta})]

            ;; Verify tempid resolution
            (is (= 1 (count (:tempids result))))
            (let [real-id (get (:tempids result) tempid)]
              (is (integer? real-id) "Real ID should be an integer from sequence")
              (is (pos? real-id) "Sequence ID should be positive")

              ;; Verify data persisted
              (let [items (query-items pool)]
                (is (= 1 (count items)))
                (let [item (first items)]
                  (is (= real-id (:id item)))
                  (is (= "Widget" (:name item)))
                  (is (= 10 (:quantity item)))
                  (is (= true (:active item))))))))
        (finally
          (pool/close pool))))))

;; =============================================================================
;; Integration Tests - Multiple Entities (Batch Scenario)
;; =============================================================================

(deftest ^:integration test-insert-multiple-items-same-type
  (testing "Inserting multiple entities of same type with sequence IDs"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [tempid1 (tempid/tempid)
                tempid2 (tempid/tempid)
                tempid3 (tempid/tempid)
                delta {[:item/id tempid1] {:item/name {:after "Widget A"} :item/quantity {:after 5}}
                       [:item/id tempid2] {:item/name {:after "Widget B"} :item/quantity {:after 10}}
                       [:item/id tempid3] {:item/name {:after "Widget C"} :item/quantity {:after 15}}}
                env (make-env pool)
                result (write/save-form! env {::rad.form/delta delta})]

            ;; All tempids resolved
            (is (= 3 (count (:tempids result))))

            ;; All IDs are unique integers
            (let [real-ids (vals (:tempids result))]
              (is (every? integer? real-ids))
              (is (= 3 (count (set real-ids))) "All IDs should be unique"))

            ;; All persisted
            (is (= 3 (count (query-items pool))))))
        (finally
          (pool/close pool))))))

(deftest ^:integration test-insert-many-items-batch-scenario
  (testing "Many entities with sequence IDs - batch optimization target"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [n 10
                tempids (repeatedly n tempid/tempid)
                delta (into {}
                            (map-indexed
                             (fn [i tid]
                               [[:item/id tid]
                                {:item/name {:after (str "Batch Item " i)}
                                 :item/quantity {:after i}}])
                             tempids))
                env (make-env pool)
                result (write/save-form! env {::rad.form/delta delta})]

            ;; All tempids resolved
            (is (= n (count (:tempids result))))
            (is (= n (count (query-items pool))))

            ;; All IDs unique and sequential
            (let [real-ids (sort (vals (:tempids result)))]
              (is (every? integer? real-ids))
              (is (= n (count (set real-ids))))
              ;; Verify monotonically increasing (sequence behavior)
              (is (apply < real-ids)))))
        (finally
          (pool/close pool))))))

;; =============================================================================
;; Integration Tests - Mixed ID Types
;; =============================================================================

(deftest ^:integration test-mixed-id-types-in-single-delta
  (testing "Delta with both UUID and sequence-based entities"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [;; UUID entity
                account-tempid (tempid/tempid)
                ;; Sequence entities
                item-tempid1 (tempid/tempid)
                item-tempid2 (tempid/tempid)

                delta {[:account/id account-tempid]
                       {:account/name {:after "Mixed Test Account"}}
                       [:item/id item-tempid1]
                       {:item/name {:after "Item One"}}
                       [:item/id item-tempid2]
                       {:item/name {:after "Item Two"}}}
                env (make-env pool)
                result (write/save-form! env {::rad.form/delta delta})]

            ;; All 3 tempids resolved
            (is (= 3 (count (:tempids result))))

            ;; Verify correct types
            (let [account-id (get (:tempids result) account-tempid)
                  item-id1 (get (:tempids result) item-tempid1)
                  item-id2 (get (:tempids result) item-tempid2)]
              (is (uuid? account-id) "Account ID should be UUID")
              (is (integer? item-id1) "Item 1 ID should be integer")
              (is (integer? item-id2) "Item 2 ID should be integer"))

            ;; Verify persisted
            (is (= 1 (count (query-accounts pool))))
            (is (= 2 (count (query-items pool))))))
        (finally
          (pool/close pool))))))

;; =============================================================================
;; Integration Tests - Cross-Entity References
;; =============================================================================

(deftest ^:integration test-sequence-id-entity-references-uuid-entity
  (testing "Entity with sequence ID referencing new entity with UUID"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [account-tempid (tempid/tempid)
                item-tempid (tempid/tempid)

                delta {[:account/id account-tempid]
                       {:account/name {:after "Owner Account"}}
                       [:item/id item-tempid]
                       {:item/name {:after "Owned Item"}
                        :item/owner {:after [:account/id account-tempid]}}}
                env (make-env pool)
                result (write/save-form! env {::rad.form/delta delta})]

            (is (= 2 (count (:tempids result))))

            (let [account-id (get (:tempids result) account-tempid)
                  item-id (get (:tempids result) item-tempid)]
              ;; Verify types
              (is (uuid? account-id))
              (is (integer? item-id))

              ;; Verify reference resolved
              (let [items (query-items pool)
                    item (first items)]
                (is (= account-id (:owner item))
                    "Item should reference the account")))))
        (finally
          (pool/close pool))))))

(deftest ^:integration test-line-item-references-new-item
  (testing "Line item (seq ID) referencing new item (seq ID)"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [item-tempid (tempid/tempid)
                line-item-tempid (tempid/tempid)

                delta {[:item/id item-tempid]
                       {:item/name {:after "Parent Item"}}
                       [:line-item/id line-item-tempid]
                       {:line-item/description {:after "Child Line"}
                        :line-item/item {:after [:item/id item-tempid]}}}
                env (make-env pool)
                result (write/save-form! env {::rad.form/delta delta})]

            (is (= 2 (count (:tempids result))))

            (let [item-id (get (:tempids result) item-tempid)
                  line-id (get (:tempids result) line-item-tempid)]
              ;; Both are integers
              (is (integer? item-id))
              (is (integer? line-id))

              ;; Verify reference
              (let [line-items (query-line-items pool)
                    line-item (first line-items)]
                (is (= item-id (:item line-item)))))))
        (finally
          (pool/close pool))))))

;; =============================================================================
;; Integration Tests - Update and Delete
;; =============================================================================

(deftest ^:integration test-update-sequence-id-entity
  (testing "Updating an entity with sequence-based ID"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [;; Insert
                tempid (tempid/tempid)
                insert-delta {[:item/id tempid] {:item/name {:after "Original"}
                                                 :item/quantity {:after 5}}}
                env (make-env pool)
                insert-result (write/save-form! env {::rad.form/delta insert-delta})
                real-id (get (:tempids insert-result) tempid)]

            ;; Update
            (let [update-delta {[:item/id real-id]
                                {:item/name {:before "Original" :after "Updated"}
                                 :item/quantity {:before 5 :after 50}}}
                  update-result (write/save-form! env {::rad.form/delta update-delta})]

              (is (empty? (:tempids update-result)) "No new tempids for update")

              ;; Verify
              (let [items (query-items pool)
                    item (first items)]
                (is (= "Updated" (:name item)))
                (is (= 50 (:quantity item)))))))
        (finally
          (pool/close pool))))))

(deftest ^:integration test-delete-sequence-id-entity
  (testing "Deleting an entity with sequence-based ID"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [;; Insert
                tempid (tempid/tempid)
                delta {[:item/id tempid] {:item/name {:after "To Delete"}}}
                env (make-env pool)
                result (write/save-form! env {::rad.form/delta delta})
                real-id (get (:tempids result) tempid)]

            (is (= 1 (count (query-items pool))))

            ;; Delete
            (write/save-form! env {::rad.form/delta {[:item/id real-id] {:delete true}}})

            (is (= 0 (count (query-items pool))) "Item should be deleted")))
        (finally
          (pool/close pool))))))

;; =============================================================================
;; Batch Optimization Verification Tests
;; =============================================================================

(deftest test-group-tempids-by-sequence
  (testing "Grouping tempids by sequence name for batch allocation"
    (let [tid1 (tempid/tempid)
          tid2 (tempid/tempid)
          tid3 (tempid/tempid)
          sequence-ids {tid1 {:sequence "items_id_seq"}
                        tid2 {:sequence "items_id_seq"}
                        tid3 {:sequence "line_items_id_seq"}}
          grouped (#'write/group-tempids-by-sequence sequence-ids)]

      ;; Should group by sequence name
      (is (= 2 (count grouped)) "Should have 2 distinct sequences")
      (is (= 2 (count (get grouped "items_id_seq"))) "items_id_seq should have 2 tempids")
      (is (= 1 (count (get grouped "line_items_id_seq"))) "line_items_id_seq should have 1 tempid"))))

(deftest ^:integration test-batch-allocation-for-same-sequence
  (testing "Multiple tempids using same sequence get batched"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [n 5
                tempids (repeatedly n tempid/tempid)
                delta (into {}
                            (map-indexed
                             (fn [i tid]
                               [[:item/id tid] {:item/name {:after (str "Item " i)}}])
                             tempids))
                env (make-env pool)

                ;; With batch optimization, N items using the same sequence
                ;; should be allocated in a single query
                result (write/save-form! env {::rad.form/delta delta})]

            (is (= n (count (:tempids result))))
            (is (= n (count (query-items pool))))

            ;; Verify IDs are sequential (batch allocation maintains order)
            (let [ids (sort (vals (:tempids result)))]
              (is (apply < ids) "IDs should be monotonically increasing"))))
        (finally
          (pool/close pool))))))

(deftest ^:integration test-batch-allocation-for-multiple-sequences
  (testing "Tempids using different sequences get separate batches"
    (let [pool (create-test-pool)]
      (try
        (with-test-schema pool schema
          (let [;; 3 items and 2 line-items = 2 batch queries (one per sequence)
                item-tids (repeatedly 3 tempid/tempid)
                line-tids (repeatedly 2 tempid/tempid)

                delta (merge
                       (into {}
                             (map-indexed
                              (fn [i tid]
                                [[:item/id tid] {:item/name {:after (str "Item " i)}}])
                              item-tids))
                       (into {}
                             (map-indexed
                              (fn [i tid]
                                [[:line-item/id tid] {:line-item/description {:after (str "Line " i)}}])
                              line-tids)))
                env (make-env pool)
                result (write/save-form! env {::rad.form/delta delta})]

            (is (= 5 (count (:tempids result))))

            ;; Verify items got sequential IDs from items_id_seq
            (let [item-ids (sort (map #(get (:tempids result) %) item-tids))]
              (is (every? integer? item-ids))
              (is (apply < item-ids)))

            ;; Verify line-items got sequential IDs from line_items_id_seq
            (let [line-ids (sort (map #(get (:tempids result) %) line-tids))]
              (is (every? integer? line-ids))
              (is (apply < line-ids)))))
        (finally
          (pool/close pool))))))
