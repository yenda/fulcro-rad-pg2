(ns com.fulcrologic.rad.database-adapters.sql.perf.stress-benchmark
  "Stress benchmarks for large batch queries and deep traversals.

   Tests:
   - Batch 100 and 1000 idents
   - 5-level deep query with ~100 leaf nodes"
  (:require
   [clojure.pprint :refer [pprint]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.perf.attributes :as perf-attrs]
   [com.fulcrologic.rad.database-adapters.sql.perf.benchmark :as bench]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [com.fulcrologic.rad.database-adapters.sql.read :as read]
   [com.fulcrologic.rad.ids :as ids]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.interface.eql :as p.eql]
   [next.jdbc :as jdbc]
   [pg.pool :as pg.pool]
   [taoensso.encore :as enc]))

(def test-db-config
  {:jdbcUrl "jdbc:postgresql://localhost:5433/fulcro-rad-pg2?user=user&password=password"})

(def key->attribute (enc/keys-by ::attr/qualified-key perf-attrs/all-attributes))

(defn seed-large-data!
  "Seed database with enough data for stress tests.

   Structure:
   - 5 orgs
   - 20 depts (4 per org)
   - 1000 employees (50 per dept)
   - 100 projects (5 per dept)
   - 500 tasks (5 per project)
   - 500 subtasks (1 per task - for 5-level depth)"
  [conn]
  (println "Seeding large dataset...")

  (let [;; Addresses (100)
        addr-ids (vec (repeatedly 100 ids/new-uuid))
        _ (println "  Creating 100 addresses...")
        _ (doseq [[i addr-id] (map-indexed vector addr-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO addresses (id, street, city, state, postal_code, country) VALUES (?, ?, ?, ?, ?, ?)"
                            addr-id (str "Street " i) (str "City " (mod i 20)) "CA" (format "%05d" i) "USA"]))

        ;; Organizations (5)
        org-ids (vec (repeatedly 5 ids/new-uuid))
        _ (println "  Creating 5 organizations...")
        _ (doseq [[i org-id] (map-indexed vector org-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO organizations (id, name, active, headquarters, employee_count) VALUES (?, ?, ?, ?, ?)"
                            org-id (str "Organization " i) true (nth addr-ids i) (* 200 (inc i))]))

        ;; Departments (20 = 4 per org)
        dept-ids (vec (for [_ (range 5) _ (range 4)] (ids/new-uuid)))
        _ (println "  Creating 20 departments...")
        _ (doseq [[i dept-id] (map-indexed vector dept-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO departments (id, name, code, active, organization, budget) VALUES (?, ?, ?, ?, ?, ?)"
                            dept-id (str "Dept " i) (str "D" (format "%03d" i)) true
                            (nth org-ids (quot i 4)) (* 50000.00 (inc i))]))

        ;; Employees (1000 = 50 per dept)
        _ (println "  Creating 1000 employees...")
        emp-ids (vec (for [_ (range 20) _ (range 50)] (ids/new-uuid)))
        _ (doseq [[i emp-id] (map-indexed vector emp-ids)]
            (let [dept-idx (quot i 50)
                  manager-id (when (pos? (mod i 50)) (nth emp-ids (* dept-idx 50)))]
              (jdbc/execute! conn
                             ["INSERT INTO employees (id, first_name, last_name, email, active, department, manager, home_address, salary) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                              emp-id (str "First" i) (str "Last" i) (str "emp" i "@test.com")
                              true (nth dept-ids dept-idx) manager-id
                              (nth addr-ids (mod i 100)) (* 50000.00 (inc (mod i 10)))])))

        ;; Projects (100 = 5 per dept)
        _ (println "  Creating 100 projects...")
        proj-ids (vec (for [_ (range 20) _ (range 5)] (ids/new-uuid)))
        _ (doseq [[i proj-id] (map-indexed vector proj-ids)]
            (let [dept-idx (quot i 5)
                  lead-idx (+ (* dept-idx 50) (mod i 50))]
              (jdbc/execute! conn
                             ["INSERT INTO projects (id, name, description, status, budget, department, lead) VALUES (?, ?, ?, ?, ?, ?, ?)"
                              proj-id (str "Project " i) (str "Description for project " i)
                              (name (nth [:status/planning :status/active :status/on-hold :status/completed] (mod i 4)))
                              (* 100000.00 (inc i)) (nth dept-ids dept-idx) (nth emp-ids lead-idx)])))

        ;; Tasks (500 = 5 per project) - these are top-level tasks
        _ (println "  Creating 500 tasks...")
        task-ids (vec (for [_ (range 100) _ (range 5)] (ids/new-uuid)))
        _ (doseq [[i task-id] (map-indexed vector task-ids)]
            (let [proj-idx (quot i 5)
                  dept-idx (quot proj-idx 5)
                  assignee-idx (+ (* dept-idx 50) (mod i 50))]
              (jdbc/execute! conn
                             ["INSERT INTO tasks (id, title, description, priority, status, estimated_hours, project, assignee, parent_task) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                              task-id (str "Task " i) (str "Task description " i)
                              (name (nth [:priority/low :priority/medium :priority/high :priority/critical] (mod i 4)))
                              (name (nth [:status/todo :status/in-progress :status/review :status/done] (mod i 4)))
                              (* 4 (inc (mod i 8)))
                              (nth proj-ids proj-idx) (nth emp-ids assignee-idx) nil])))

        ;; Subtasks (500 = 1 per task) - for 5-level depth
        ;; NOTE: Subtasks don't have a direct project association - they inherit it through parent_task
        ;; This ensures :project/tasks only returns top-level tasks
        _ (println "  Creating 500 subtasks...")
        subtask-ids (vec (repeatedly 500 ids/new-uuid))
        _ (doseq [[i subtask-id] (map-indexed vector subtask-ids)]
            (let [parent-task-id (nth task-ids i)
                  proj-idx (quot i 5)
                  dept-idx (quot proj-idx 5)
                  assignee-idx (+ (* dept-idx 50) (mod (inc i) 50))]
              (jdbc/execute! conn
                             ["INSERT INTO tasks (id, title, description, priority, status, estimated_hours, project, assignee, parent_task) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                              subtask-id (str "Subtask " i) (str "Subtask description " i)
                              (name (nth [:priority/low :priority/medium :priority/high] (mod i 3)))
                              (name (nth [:status/todo :status/in-progress :status/done] (mod i 3)))
                              (* 2 (inc (mod i 4)))
                              nil (nth emp-ids assignee-idx) parent-task-id])))]

    (println "  Data seeding complete!")
    {:addr-ids addr-ids :org-ids org-ids :dept-ids dept-ids
     :emp-ids emp-ids :proj-ids proj-ids :task-ids task-ids :subtask-ids subtask-ids}))

(defn run-stress-benchmarks
  "Run stress benchmarks with pg2 driver."
  []
  (let [schema-name (str "test_stress_" (System/currentTimeMillis))
        jdbc-ds (jdbc/get-datasource test-db-config)
        jdbc-conn (jdbc/get-connection jdbc-ds)
        pg2-pool* (atom nil)]
    (try
      ;; Setup schema using JDBC
      (jdbc/execute! jdbc-conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! jdbc-conn [(str "SET search_path TO " schema-name)])
      (println "Creating schema...")
      (doseq [s (mig/automatic-schema :perf perf-attrs/all-attributes)]
        (jdbc/execute! jdbc-conn [s]))

      ;; Seed data
      (let [{:keys [org-ids emp-ids]} (seed-large-data! jdbc-conn)

            ;; Create pg2 pool
            pg2-pool (pg2/create-pool! {:pg2/config {:host "localhost"
                                                     :port 5433
                                                     :user "user"
                                                     :password "password"
                                                     :database "fulcro-rad-pg2"
                                                     :pg-params {"search_path" schema-name}}})
            _ (reset! pg2-pool* pg2-pool)

            ;; Build pathom env
            resolvers (read/generate-resolvers perf-attrs/all-attributes :perf)
            pathom-env (-> (pci/register resolvers)
                           (assoc ::attr/key->attribute key->attribute
                                  ::rad.sql/connection-pools {:perf pg2-pool}))
            pathom-query #(p.eql/process pathom-env %)
            results (atom {})]

        (bench/print-header "PG2 STRESS BENCHMARKS")

        ;; === BATCH QUERIES ===
        (println "Large batch queries:")

        (swap! results assoc :batch-100
               (bench/benchmark! "Batch 100 employees"
                                 #(pathom-query (vec (for [id (take 100 emp-ids)]
                                                       {[:employee/id id]
                                                        [:employee/id :employee/first-name :employee/last-name
                                                         :employee/email :employee/active]})))
                                 {:iterations 20 :warmup 3}))

        (swap! results assoc :batch-1000
               (bench/benchmark! "Batch 1000 employees"
                                 #(pathom-query (vec (for [id emp-ids]
                                                       {[:employee/id id]
                                                        [:employee/id :employee/first-name :employee/last-name
                                                         :employee/email :employee/active]})))
                                 {:iterations 10 :warmup 2}))

        (swap! results assoc :batch-100-with-join
               (bench/benchmark! "Batch 100 with to-one join"
                                 #(pathom-query (vec (for [id (take 100 emp-ids)]
                                                       {[:employee/id id]
                                                        [:employee/id :employee/first-name
                                                         {:employee/department [:department/id :department/name]}]})))
                                 {:iterations 20 :warmup 3}))

        ;; === DEEP TRAVERSAL ===
        (println "\nDeep traversal queries:")

        ;; 5-level: org -> depts -> projects -> tasks -> subtasks
        ;; With 5 orgs * 4 depts * 5 projects * 5 tasks * 1 subtask = 500 leaf nodes per org
        ;; Query first org = ~100 subtasks
        (swap! results assoc :deep-5-level
               (bench/benchmark! "5-level deep (org->dept->proj->task->subtask)"
                                 #(pathom-query [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name
                                                   {:organization/departments
                                                    [:department/id :department/name
                                                     {:department/projects
                                                      [:project/id :project/name
                                                       {:project/tasks
                                                        [:task/id :task/title
                                                         {:task/subtasks
                                                          [:task/id :task/title :task/priority]}]}]}]}]}])
                                 {:iterations 20 :warmup 3}))

        ;; 4-level with more fields
        (swap! results assoc :deep-4-level-wide
               (bench/benchmark! "4-level deep with many fields"
                                 #(pathom-query [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name :organization/active
                                                   :organization/employee-count
                                                   {:organization/departments
                                                    [:department/id :department/name :department/code
                                                     :department/budget :department/active
                                                     {:department/projects
                                                      [:project/id :project/name :project/description
                                                       :project/status :project/budget
                                                       {:project/tasks
                                                        [:task/id :task/title :task/description
                                                         :task/priority :task/status :task/estimated-hours]}]}]}]}])
                                 {:iterations 20 :warmup 3}))

        (println "\n=== STRESS TEST RESULTS ===")
        (pprint @results)
        @results)

      (finally
        (jdbc/execute! jdbc-conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close jdbc-conn)
        (when-let [p @pg2-pool*]
          (pg.pool/close p))))))

(comment
  (run-stress-benchmarks))
