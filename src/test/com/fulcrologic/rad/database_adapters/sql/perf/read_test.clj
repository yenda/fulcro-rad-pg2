(ns com.fulcrologic.rad.database-adapters.sql.perf.read-test
  "Performance benchmarks for Pathom3 resolver reads.

   Benchmarks range from simple ID lookups to complex multi-level
   traversals with sorting. Run with:

   clojure -A:test:dev -M -e \"(require 'com.fulcrologic.rad.database-adapters.sql.perf.read-test) (com.fulcrologic.rad.database-adapters.sql.perf.read-test/run-all-benchmarks)\""
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.perf.attributes :as perf-attrs]
   [com.fulcrologic.rad.database-adapters.sql.perf.benchmark :as bench]
   [com.fulcrologic.rad.database-adapters.sql.resolvers-pathom3 :as resolvers-p3]
   [com.fulcrologic.rad.database-adapters.sql.result-set :as sql.rs]
   [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]
   [com.fulcrologic.rad.ids :as ids]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.interface.eql :as p.eql]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [taoensso.encore :as enc]))

;; Initialize result set coercion for PostgreSQL arrays
(sql.rs/coerce-result-sets!)

;; =============================================================================
;; Configuration
;; =============================================================================

(def test-db-config
  {:jdbcUrl "jdbc:postgresql://localhost:5432/fulcro-rad-pg2?user=user&password=password"})

(def key->attribute (enc/keys-by ::attr/qualified-key perf-attrs/all-attributes))

(def ^:dynamic *benchmark-iterations* 50)
(def ^:dynamic *benchmark-warmup* 5)

;; =============================================================================
;; Test Data Seeding
;; =============================================================================

(def jdbc-opts {:builder-fn rs/as-unqualified-lower-maps})

(defn generate-test-schema-name []
  (str "test_perf_read_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn seed-test-data!
  "Seed the database with test data. Returns entity IDs map."
  [conn]
  (let [;; Addresses (20)
        addr-ids (vec (repeatedly 20 ids/new-uuid))
        _ (doseq [[i addr-id] (map-indexed vector addr-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO addresses (id, street, city, state, postal_code, country) VALUES (?, ?, ?, ?, ?, ?)"
                            addr-id (str "Street " i) (str "City " i) "CA" (format "%05d" i) "USA"]))

        ;; Skills (10)
        skill-ids (vec (repeatedly 10 ids/new-uuid))
        categories [:category/technical :category/management :category/communication :category/leadership]
        levels [:level/beginner :level/intermediate :level/advanced :level/expert]
        _ (doseq [[i skill-id] (map-indexed vector skill-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO skills (id, name, category, level) VALUES (?, ?, ?, ?)"
                            skill-id (str "Skill " i) (name (nth categories (mod i 4))) (name (nth levels (mod i 4)))]))

        ;; Organizations (3)
        org-ids (vec (repeatedly 3 ids/new-uuid))
        _ (doseq [[i org-id] (map-indexed vector org-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO organizations (id, name, active, headquarters, employee_count) VALUES (?, ?, ?, ?, ?)"
                            org-id (str "Organization " i) true (nth addr-ids i) (* 100 (inc i))]))

        ;; Departments (9 = 3 per org)
        dept-ids (vec (for [_ (range 3) _ (range 3)] (ids/new-uuid)))
        _ (doseq [[i dept-id] (map-indexed vector dept-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO departments (id, name, code, active, organization, budget) VALUES (?, ?, ?, ?, ?, ?)"
                            dept-id (str "Dept " i) (str "D" i) true (nth org-ids (quot i 3)) (* 10000.00 (inc i))]))

        ;; Employees (90 = 10 per dept)
        emp-ids (vec (for [_ (range 9) _ (range 10)] (ids/new-uuid)))
        _ (doseq [[i emp-id] (map-indexed vector emp-ids)]
            (let [dept-idx (quot i 10)
                  manager-id (when (pos? (mod i 10)) (nth emp-ids (* dept-idx 10)))]
              (jdbc/execute! conn
                             ["INSERT INTO employees (id, first_name, last_name, email, active, department, manager, home_address, salary) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                              emp-id (str "First" i) (str "Last" i) (str "emp" i "@test.com")
                              true (nth dept-ids dept-idx) manager-id (nth addr-ids (mod i 20)) (* 50000.00 (inc (mod i 10)))])))

        ;; Set department managers
        _ (doseq [[i dept-id] (map-indexed vector dept-ids)]
            (jdbc/execute! conn ["UPDATE departments SET manager = ? WHERE id = ?" (nth emp-ids (* i 10)) dept-id]))

        ;; Employee-skill associations (270 = 3 per employee)
        _ (doseq [[i emp-id] (map-indexed vector emp-ids)]
            (doseq [offset (range 3)]
              (jdbc/execute! conn
                             ["INSERT INTO employee_skills (id, employee, skill, proficiency) VALUES (?, ?, ?, ?)"
                              (ids/new-uuid) emp-id (nth skill-ids (mod (+ i offset) 10))
                              (name (nth [:proficiency/learning :proficiency/competent :proficiency/proficient :proficiency/expert] (mod (+ i offset) 4)))])))

        ;; Projects (18 = 2 per dept)
        proj-ids (vec (for [_ (range 9) _ (range 2)] (ids/new-uuid)))
        _ (doseq [[i proj-id] (map-indexed vector proj-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO projects (id, name, description, status, budget, department, lead) VALUES (?, ?, ?, ?, ?, ?, ?)"
                            proj-id (str "Project " i) (str "Description " i)
                            (name (nth [:status/planning :status/active :status/on-hold] (mod i 3)))
                            (* 100000.00 (inc i)) (nth dept-ids (quot i 2)) (nth emp-ids (* (quot i 2) 10))]))

        ;; Tasks (180 = 10 per project, half are subtasks)
        task-ids (vec (for [_ (range 18) _ (range 10)] (ids/new-uuid)))
        _ (doseq [[i task-id] (map-indexed vector task-ids)]
            (let [proj-idx (quot i 10)
                  task-in-proj (mod i 10)
                  parent-id (when (>= task-in-proj 5) (nth task-ids (+ (* proj-idx 10) (- task-in-proj 5))))
                  assignee-idx (+ (* (quot proj-idx 2) 10) (mod i 10))]
              (jdbc/execute! conn
                             ["INSERT INTO tasks (id, title, description, priority, status, estimated_hours, project, assignee, parent_task) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                              task-id (str "Task " i) (str "Task description " i)
                              (name (nth [:priority/low :priority/medium :priority/high :priority/critical] (mod i 4)))
                              (name (nth [:status/todo :status/in-progress :status/review :status/done] (mod i 4)))
                              (* 4 (inc (mod i 5)))
                              (nth proj-ids proj-idx) (nth emp-ids assignee-idx) parent-id])))]

    {:addr-ids addr-ids :skill-ids skill-ids :org-ids org-ids
     :dept-ids dept-ids :emp-ids emp-ids :proj-ids proj-ids :task-ids task-ids}))

(defn with-test-db
  "Run function with isolated test database schema and seeded data."
  [f]
  (let [ds (jdbc/get-datasource test-db-config)
        schema-name (generate-test-schema-name)
        conn (jdbc/get-connection ds)]
    (try
      (jdbc/execute! conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! conn [(str "SET search_path TO " schema-name)])
      (doseq [s (mig/automatic-schema :perf (vendor/->PostgreSQLAdapter) perf-attrs/all-attributes)]
        (jdbc/execute! conn [s]))
      (let [entity-ids (seed-test-data! conn)
            resolvers (resolvers-p3/generate-resolvers perf-attrs/all-attributes :perf)
            pathom-env (-> (pci/register resolvers)
                           (assoc ::attr/key->attribute key->attribute
                                  ::rad.sql/connection-pools {:perf conn}
                                  ::rad.sql/adapters {:perf (vendor/->PostgreSQLAdapter)}
                                  ::rad.sql/default-adapter (vendor/->PostgreSQLAdapter)))]
        (f conn pathom-env entity-ids))
      (finally
        (jdbc/execute! conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close conn)))))

;; =============================================================================
;; Query Helper
;; =============================================================================

(defn pathom-query [env query]
  (p.eql/process env query))

;; =============================================================================
;; Benchmark Scenarios - Simple to Complex
;; =============================================================================

(defn run-read-benchmarks
  "Run all read benchmarks and return results map."
  [env {:keys [org-ids dept-ids emp-ids proj-ids task-ids]}]
  (bench/print-header "READ BENCHMARKS - Pathom3 Resolvers")

  (let [opts {:iterations *benchmark-iterations* :warmup *benchmark-warmup*}
        results (atom {})]

    ;; === SIMPLE: Single entity by ID ===
    (println "Simple queries (single entity):")

    (swap! results assoc :simple-by-id
           (bench/benchmark! "Single entity by ID"
                             #(pathom-query env [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name :organization/active]}])
                             opts))

    (swap! results assoc :simple-scalar-only
           (bench/benchmark! "Scalar fields only"
                             #(pathom-query env [{[:employee/id (first emp-ids)]
                                                  [:employee/id :employee/first-name :employee/last-name
                                                   :employee/email :employee/active]}])
                             opts))

    ;; === MEDIUM: Single entity with one join ===
    (println "\nMedium queries (one join):")

    (swap! results assoc :medium-to-one
           (bench/benchmark! "To-one join (emp->dept)"
                             #(pathom-query env [{[:employee/id (first emp-ids)]
                                                  [:employee/id :employee/first-name
                                                   {:employee/department [:department/id :department/name]}]}])
                             opts))

    (swap! results assoc :medium-to-many-small
           (bench/benchmark! "To-many join small (dept->10 emps)"
                             #(pathom-query env [{[:department/id (first dept-ids)]
                                                  [:department/id :department/name
                                                   {:department/employees [:employee/id :employee/first-name]}]}])
                             opts))

    ;; === COMPLEX: Multiple joins ===
    (println "\nComplex queries (multiple joins):")

    (swap! results assoc :complex-2-level
           (bench/benchmark! "2-level deep (org->depts)"
                             #(pathom-query env [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name
                                                   {:organization/departments
                                                    [:department/id :department/name :department/code]}]}])
                             opts))

    (swap! results assoc :complex-3-level
           (bench/benchmark! "3-level deep (org->depts->projs)"
                             #(pathom-query env [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name
                                                   {:organization/departments
                                                    [:department/id :department/name
                                                     {:department/projects
                                                      [:project/id :project/name :project/status]}]}]}])
                             opts))

    (swap! results assoc :complex-4-level
           (bench/benchmark! "4-level deep (org->depts->projs->tasks)"
                             #(pathom-query env [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name
                                                   {:organization/departments
                                                    [:department/id :department/name
                                                     {:department/projects
                                                      [:project/id :project/name
                                                       {:project/tasks
                                                        [:task/id :task/title :task/priority]}]}]}]}])
                             opts))

    ;; === BATCH: Multiple entities ===
    (println "\nBatch queries:")

    (swap! results assoc :batch-10
           (bench/benchmark! "Batch 10 entities"
                             #(pathom-query env (vec (for [id (take 10 emp-ids)]
                                                       {[:employee/id id]
                                                        [:employee/id :employee/first-name :employee/email]})))
                             opts))

    (swap! results assoc :batch-50
           (bench/benchmark! "Batch 50 entities"
                             #(pathom-query env (vec (for [id (take 50 emp-ids)]
                                                       {[:employee/id id]
                                                        [:employee/id :employee/first-name :employee/email]})))
                             opts))

    (swap! results assoc :batch-with-join
           (bench/benchmark! "Batch 50 with to-one join"
                             #(pathom-query env (vec (for [id (take 50 emp-ids)]
                                                       {[:employee/id id]
                                                        [:employee/id :employee/first-name
                                                         {:employee/department [:department/id :department/name]}]})))
                             opts))

    ;; === SELF-REFERENCE ===
    (println "\nSelf-reference queries:")

    (swap! results assoc :self-ref-to-one
           (bench/benchmark! "Self-ref to-one (emp->manager)"
                             #(pathom-query env [{[:employee/id (second emp-ids)]
                                                  [:employee/id :employee/first-name
                                                   {:employee/manager [:employee/id :employee/first-name]}]}])
                             opts))

    (swap! results assoc :self-ref-to-many
           (bench/benchmark! "Self-ref to-many (manager->reports)"
                             #(pathom-query env [{[:employee/id (first emp-ids)]
                                                  [:employee/id :employee/first-name
                                                   {:employee/direct-reports [:employee/id :employee/first-name]}]}])
                             opts))

    (swap! results assoc :self-ref-subtasks
           (bench/benchmark! "Self-ref subtasks (task->subtasks)"
                             #(pathom-query env [{[:task/id (first task-ids)]
                                                  [:task/id :task/title
                                                   {:task/subtasks [:task/id :task/title]}]}])
                             opts))

    @results))

;; =============================================================================
;; Test Entry Points
;; =============================================================================

(deftest read-benchmarks-test
  (testing "Read benchmarks complete without errors"
    (with-test-db
      (fn [_conn env entity-ids]
        (let [results (run-read-benchmarks env entity-ids)]
          ;; Basic sanity checks
          (is (pos? (get-in results [:simple-by-id :mean])))
          (is (pos? (get-in results [:complex-4-level :mean])))
          ;; Complex should be slower than simple
          (is (< (get-in results [:simple-by-id :mean])
                 (get-in results [:complex-4-level :mean]))))))))

(defn run-all-benchmarks
  "Run all read benchmarks. Call from REPL or command line."
  []
  (with-test-db
    (fn [_conn env entity-ids]
      (run-read-benchmarks env entity-ids))))
