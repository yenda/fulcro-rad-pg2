(ns com.fulcrologic.rad.database-adapters.sql.perf.write-test
  "Performance benchmarks for save-form! writes.

   Benchmarks range from simple single-entity inserts to complex
   multi-entity deltas with relationships. Run with:

   clojure -A:test:dev -M -e \"(require 'com.fulcrologic.rad.database-adapters.sql.perf.write-test) (com.fulcrologic.rad.database-adapters.sql.perf.write-test/run-all-benchmarks)\""
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.perf.attributes :as perf-attrs]
   [com.fulcrologic.rad.database-adapters.sql.perf.benchmark :as bench]
   [com.fulcrologic.rad.database-adapters.sql.resolvers :as resolvers]
   [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [taoensso.encore :as enc]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def test-db-config
  {:jdbcUrl "jdbc:postgresql://localhost:5433/fulcro-rad-pg2?user=user&password=password"})

(def key->attribute (enc/keys-by ::attr/qualified-key perf-attrs/all-attributes))

(def ^:dynamic *benchmark-iterations* 20)
(def ^:dynamic *benchmark-warmup* 3)

;; =============================================================================
;; Test Fixture
;; =============================================================================

(def jdbc-opts {:builder-fn rs/as-unqualified-lower-maps})

(defn generate-test-schema-name []
  (str "test_perf_write_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn with-test-db
  "Run function with isolated test database schema.
   For write benchmarks, we recreate the schema between iterations."
  [f]
  (let [ds (jdbc/get-datasource test-db-config)
        schema-name (generate-test-schema-name)
        conn (jdbc/get-connection ds)]
    (try
      (jdbc/execute! conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! conn [(str "SET search_path TO " schema-name)])
      (doseq [s (mig/automatic-schema :perf (vendor/->PostgreSQLAdapter) perf-attrs/all-attributes)]
        (jdbc/execute! conn [s]))
      (let [env {::attr/key->attribute key->attribute
                 ::rad.sql/connection-pools {:perf conn}
                 ::rad.sql/adapters {:perf (vendor/->PostgreSQLAdapter)}
                 ::rad.sql/default-adapter (vendor/->PostgreSQLAdapter)}]
        (f conn env))
      (finally
        (jdbc/execute! conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close conn)))))

(defn clear-tables!
  "Clear all tables for next benchmark iteration."
  [conn]
  (doseq [table ["tasks" "projects" "employee_skills" "employees"
                 "departments" "organizations" "skills" "addresses"]]
    (jdbc/execute! conn [(str "TRUNCATE TABLE " table " CASCADE")])))

(defn count-rows [conn table]
  (:count (first (jdbc/execute! conn [(str "SELECT COUNT(*) as count FROM " table)] jdbc-opts))))

;; =============================================================================
;; Delta Builders - Simple to Complex
;; =============================================================================

(defn make-simple-insert-delta
  "Single entity insert - simplest possible delta."
  []
  (let [tid (tempid/tempid)]
    {[:address/id tid]
     {:address/street {:after "123 Main St"}
      :address/city {:after "Test City"}
      :address/state {:after "CA"}
      :address/postal-code {:after "12345"}
      :address/country {:after "USA"}}}))

(defn make-simple-update-delta
  "Single field update on existing entity."
  [existing-id]
  {[:address/id existing-id]
   {:address/street {:before "123 Main St"
                     :after "456 Oak Ave"}}})

(defn make-medium-insert-delta
  "Entity with one to-one relationship."
  []
  (let [emp-tid (tempid/tempid)
        addr-tid (tempid/tempid)
        dept-tid (tempid/tempid)
        org-tid (tempid/tempid)]
    {[:organization/id org-tid]
     {:organization/name {:after "Acme Corp"}
      :organization/active {:after true}}

     [:department/id dept-tid]
     {:department/name {:after "Engineering"}
      :department/code {:after "ENG"}
      :department/active {:after true}
      :department/organization {:after [:organization/id org-tid]}}

     [:address/id addr-tid]
     {:address/street {:after "789 Work St"}
      :address/city {:after "Work City"}
      :address/state {:after "NY"}
      :address/country {:after "USA"}}

     [:employee/id emp-tid]
     {:employee/first-name {:after "John"}
      :employee/last-name {:after "Doe"}
      :employee/email {:after "john@acme.com"}
      :employee/active {:after true}
      :employee/department {:after [:department/id dept-tid]}
      :employee/home-address {:after [:address/id addr-tid]}}}))

(defn make-complex-hierarchy-delta
  "4-level deep hierarchy: Org -> Dept -> Project -> Tasks."
  [num-tasks]
  (let [org-tid (tempid/tempid)
        dept-tid (tempid/tempid)
        proj-tid (tempid/tempid)
        task-tids (repeatedly num-tasks tempid/tempid)]
    (merge
     {[:organization/id org-tid]
      {:organization/name {:after "Deep Org"}
       :organization/active {:after true}}

      [:department/id dept-tid]
      {:department/name {:after "Dev Team"}
       :department/code {:after "DEV"}
       :department/active {:after true}
       :department/organization {:after [:organization/id org-tid]}}

      [:project/id proj-tid]
      {:project/name {:after "Big Project"}
       :project/status {:after :status/active}
       :project/department {:after [:department/id dept-tid]}}}

     (into {}
           (map-indexed
            (fn [i tid]
              [[:task/id tid]
               {:task/title {:after (str "Task " i)}
                :task/priority {:after (nth [:priority/low :priority/medium :priority/high] (mod i 3))}
                :task/status {:after :status/todo}
                :task/estimated-hours {:after (* 4 (inc i))}
                :task/project {:after [:project/id proj-tid]}}])
            task-tids)))))

(defn make-batch-insert-delta
  "Batch of N simple entities."
  [n]
  (into {}
        (for [i (range n)]
          (let [tid (tempid/tempid)]
            [[:address/id tid]
             {:address/street {:after (str "Street " i)}
              :address/city {:after "Batch City"}
              :address/state {:after "TX"}
              :address/postal-code {:after (format "%05d" i)}
              :address/country {:after "USA"}}]))))

(defn make-batch-with-refs-delta
  "Batch of N employees with addresses (2N entities total)."
  [n]
  (let [org-tid (tempid/tempid)
        dept-tid (tempid/tempid)]
    (merge
     {[:organization/id org-tid]
      {:organization/name {:after "Batch Org"}
       :organization/active {:after true}}

      [:department/id dept-tid]
      {:department/name {:after "Batch Dept"}
       :department/code {:after "BATCH"}
       :department/active {:after true}
       :department/organization {:after [:organization/id org-tid]}}}

     (into {}
           (mapcat
            (fn [i]
              (let [emp-tid (tempid/tempid)
                    addr-tid (tempid/tempid)]
                [[[:address/id addr-tid]
                  {:address/street {:after (str "Home " i)}
                   :address/city {:after "Batch City"}
                   :address/state {:after "CA"}
                   :address/country {:after "USA"}}]
                 [[:employee/id emp-tid]
                  {:employee/first-name {:after (str "Emp" i)}
                   :employee/last-name {:after "Batch"}
                   :employee/email {:after (str "emp" i "@batch.com")}
                   :employee/active {:after true}
                   :employee/department {:after [:department/id dept-tid]}
                   :employee/home-address {:after [:address/id addr-tid]}}]]))
            (range n))))))

(defn make-mixed-operations-delta
  "Delta with inserts, updates, and deletes."
  [existing-ids]
  (let [{:keys [update-ids delete-ids]} existing-ids
        new-tid (tempid/tempid)]
    (merge
     ;; Insert
     {[:address/id new-tid]
      {:address/street {:after "New Street"}
       :address/city {:after "New City"}
       :address/state {:after "FL"}
       :address/country {:after "USA"}}}

     ;; Updates
     (into {}
           (map-indexed
            (fn [i id]
              [[:address/id id]
               {:address/street {:before (str "Street " i)
                                 :after (str "Updated " i)}}])
            update-ids))

     ;; Deletes
     (into {}
           (map (fn [id] [[:address/id id] {:delete true}]) delete-ids)))))

(defn make-self-ref-delta
  "Create tasks with parent-child relationships (self-reference)."
  [num-parents num-children-per-parent]
  (let [org-tid (tempid/tempid)
        dept-tid (tempid/tempid)
        proj-tid (tempid/tempid)
        parent-tids (vec (repeatedly num-parents tempid/tempid))]
    (merge
     {[:organization/id org-tid]
      {:organization/name {:after "Self-Ref Org"}
       :organization/active {:after true}}

      [:department/id dept-tid]
      {:department/name {:after "Self-Ref Dept"}
       :department/code {:after "SELF"}
       :department/active {:after true}
       :department/organization {:after [:organization/id org-tid]}}

      [:project/id proj-tid]
      {:project/name {:after "Self-Ref Project"}
       :project/status {:after :status/active}
       :project/department {:after [:department/id dept-tid]}}}

     ;; Parent tasks
     (into {}
           (map-indexed
            (fn [i tid]
              [[:task/id tid]
               {:task/title {:after (str "Parent " i)}
                :task/priority {:after :priority/high}
                :task/status {:after :status/in-progress}
                :task/project {:after [:project/id proj-tid]}}])
            parent-tids))

     ;; Child tasks
     (into {}
           (for [parent-idx (range num-parents)
                 child-idx (range num-children-per-parent)]
             (let [child-tid (tempid/tempid)]
               [[:task/id child-tid]
                {:task/title {:after (str "Child " parent-idx "-" child-idx)}
                 :task/priority {:after :priority/low}
                 :task/status {:after :status/todo}
                 :task/project {:after [:project/id proj-tid]}
                 :task/parent {:after [:task/id (nth parent-tids parent-idx)]}}]))))))

;; =============================================================================
;; Benchmark Scenarios
;; =============================================================================

(defn run-write-benchmarks
  "Run all write benchmarks and return results map."
  [conn env]
  (bench/print-header "WRITE BENCHMARKS - save-form!")

  (let [opts {:iterations *benchmark-iterations* :warmup *benchmark-warmup*}
        results (atom {})]

    ;; === SIMPLE INSERTS ===
    (println "Simple operations:")

    (swap! results assoc :simple-insert
           (bench/benchmark! "Single entity insert"
                             #(resolvers/save-form! env {::rad.form/delta (make-simple-insert-delta)})
                             (assoc opts :teardown #(clear-tables! conn))))

    ;; For update test, we need an existing entity
    (let [setup-result (resolvers/save-form! env {::rad.form/delta (make-simple-insert-delta)})
          existing-id (first (vals (:tempids setup-result)))]
      (swap! results assoc :simple-update
             (bench/benchmark! "Single field update"
                               #(resolvers/save-form! env {::rad.form/delta (make-simple-update-delta existing-id)})
                               opts)))
    (clear-tables! conn)

    ;; === MEDIUM COMPLEXITY ===
    (println "\nMedium complexity (entity with refs):")

    (swap! results assoc :medium-insert
           (bench/benchmark! "Entity with to-one refs (4 entities)"
                             #(resolvers/save-form! env {::rad.form/delta (make-medium-insert-delta)})
                             (assoc opts :teardown #(clear-tables! conn))))

    ;; === HIERARCHIES ===
    (println "\nDeep hierarchies:")

    (swap! results assoc :hierarchy-5-tasks
           (bench/benchmark! "4-level hierarchy (5 tasks)"
                             #(resolvers/save-form! env {::rad.form/delta (make-complex-hierarchy-delta 5)})
                             (assoc opts :teardown #(clear-tables! conn))))

    (swap! results assoc :hierarchy-20-tasks
           (bench/benchmark! "4-level hierarchy (20 tasks)"
                             #(resolvers/save-form! env {::rad.form/delta (make-complex-hierarchy-delta 20)})
                             (assoc opts :teardown #(clear-tables! conn))))

    (swap! results assoc :hierarchy-50-tasks
           (bench/benchmark! "4-level hierarchy (50 tasks)"
                             #(resolvers/save-form! env {::rad.form/delta (make-complex-hierarchy-delta 50)})
                             (assoc opts :teardown #(clear-tables! conn))))

    ;; === BATCH INSERTS ===
    (println "\nBatch inserts:")

    (swap! results assoc :batch-10
           (bench/benchmark! "Batch 10 simple entities"
                             #(resolvers/save-form! env {::rad.form/delta (make-batch-insert-delta 10)})
                             (assoc opts :teardown #(clear-tables! conn))))

    (swap! results assoc :batch-50
           (bench/benchmark! "Batch 50 simple entities"
                             #(resolvers/save-form! env {::rad.form/delta (make-batch-insert-delta 50)})
                             (assoc opts :teardown #(clear-tables! conn))))

    (swap! results assoc :batch-100
           (bench/benchmark! "Batch 100 simple entities"
                             #(resolvers/save-form! env {::rad.form/delta (make-batch-insert-delta 100)})
                             (assoc opts :teardown #(clear-tables! conn))))

    ;; === BATCH WITH REFS ===
    (println "\nBatch with relationships:")

    (swap! results assoc :batch-refs-10
           (bench/benchmark! "Batch 10 employees + addresses (22 entities)"
                             #(resolvers/save-form! env {::rad.form/delta (make-batch-with-refs-delta 10)})
                             (assoc opts :teardown #(clear-tables! conn))))

    (swap! results assoc :batch-refs-25
           (bench/benchmark! "Batch 25 employees + addresses (52 entities)"
                             #(resolvers/save-form! env {::rad.form/delta (make-batch-with-refs-delta 25)})
                             (assoc opts :teardown #(clear-tables! conn))))

    (swap! results assoc :batch-refs-50
           (bench/benchmark! "Batch 50 employees + addresses (102 entities)"
                             #(resolvers/save-form! env {::rad.form/delta (make-batch-with-refs-delta 50)})
                             (assoc opts :teardown #(clear-tables! conn))))

    ;; === MIXED OPERATIONS ===
    (println "\nMixed operations (insert + update + delete):")

    ;; Setup: create entities to update/delete
    (let [setup-delta (make-batch-insert-delta 20)
          setup-result (resolvers/save-form! env {::rad.form/delta setup-delta})
          all-ids (vec (vals (:tempids setup-result)))
          update-ids (take 10 all-ids)
          delete-ids (drop 10 all-ids)]

      (swap! results assoc :mixed-ops
             (bench/benchmark! "1 insert + 10 updates + 10 deletes"
                               #(resolvers/save-form! env {::rad.form/delta
                                                           (make-mixed-operations-delta
                                                            {:update-ids update-ids :delete-ids delete-ids})})
                               opts)))
    (clear-tables! conn)

    ;; === SELF-REFERENCES ===
    (println "\nSelf-referential (parent-child tasks):")

    (swap! results assoc :self-ref-small
           (bench/benchmark! "5 parents x 2 children (13 entities)"
                             #(resolvers/save-form! env {::rad.form/delta (make-self-ref-delta 5 2)})
                             (assoc opts :teardown #(clear-tables! conn))))

    (swap! results assoc :self-ref-medium
           (bench/benchmark! "10 parents x 5 children (53 entities)"
                             #(resolvers/save-form! env {::rad.form/delta (make-self-ref-delta 10 5)})
                             (assoc opts :teardown #(clear-tables! conn))))

    (swap! results assoc :self-ref-large
           (bench/benchmark! "20 parents x 10 children (203 entities)"
                             #(resolvers/save-form! env {::rad.form/delta (make-self-ref-delta 20 10)})
                             (assoc opts :teardown #(clear-tables! conn))))

    @results))

;; =============================================================================
;; Test Entry Points
;; =============================================================================

(deftest write-benchmarks-test
  (testing "Write benchmarks complete without errors"
    (with-test-db
      (fn [conn env]
        (let [results (run-write-benchmarks conn env)]
          ;; Basic sanity checks
          (is (pos? (get-in results [:simple-insert :mean])))
          (is (pos? (get-in results [:batch-100 :mean])))
          ;; Batch should take longer than single
          (is (< (get-in results [:simple-insert :mean])
                 (get-in results [:batch-100 :mean]))))))))

(defn run-all-benchmarks
  "Run all write benchmarks. Call from REPL or command line."
  []
  (with-test-db
    (fn [conn env]
      (run-write-benchmarks conn env))))
