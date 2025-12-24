(ns com.fulcrologic.rad.database-adapters.sql.perf.pg2-benchmark
  "Benchmark Pathom3 resolvers using pg2 driver."
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
  {:jdbcUrl "jdbc:postgresql://localhost:5432/fulcro-rad-pg2?user=user&password=password"})

(def key->attribute (enc/keys-by ::attr/qualified-key perf-attrs/all-attributes))

(defn seed-basic-data!
  "Seed minimal data for benchmarks."
  [conn]
  (let [addr-ids (vec (repeatedly 20 ids/new-uuid))
        _ (doseq [[i addr-id] (map-indexed vector addr-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO addresses (id, street, city, state, postal_code, country) VALUES (?, ?, ?, ?, ?, ?)"
                            addr-id (str "Street " i) (str "City " i) "CA" (format "%05d" i) "USA"]))

        org-ids (vec (repeatedly 3 ids/new-uuid))
        _ (doseq [[i org-id] (map-indexed vector org-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO organizations (id, name, active, headquarters, employee_count) VALUES (?, ?, ?, ?, ?)"
                            org-id (str "Organization " i) true (nth addr-ids i) (* 100 (inc i))]))

        dept-ids (vec (for [_ (range 3) _ (range 3)] (ids/new-uuid)))
        _ (doseq [[i dept-id] (map-indexed vector dept-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO departments (id, name, code, active, organization, budget) VALUES (?, ?, ?, ?, ?, ?)"
                            dept-id (str "Dept " i) (str "D" i) true (nth org-ids (quot i 3)) (* 10000.00 (inc i))]))

        emp-ids (vec (for [_ (range 9) _ (range 10)] (ids/new-uuid)))
        _ (doseq [[i emp-id] (map-indexed vector emp-ids)]
            (let [dept-idx (quot i 10)
                  manager-id (when (pos? (mod i 10)) (nth emp-ids (* dept-idx 10)))]
              (jdbc/execute! conn
                             ["INSERT INTO employees (id, first_name, last_name, email, active, department, manager, home_address, salary) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                              emp-id (str "First" i) (str "Last" i) (str "emp" i "@test.com")
                              true (nth dept-ids dept-idx) manager-id (nth addr-ids (mod i 20)) (* 50000.00 (inc (mod i 10)))])))

        ;; Projects (18 = 2 per dept)
        proj-ids (vec (for [_ (range 9) _ (range 2)] (ids/new-uuid)))
        _ (doseq [[i proj-id] (map-indexed vector proj-ids)]
            (jdbc/execute! conn
                           ["INSERT INTO projects (id, name, description, status, budget, department, lead) VALUES (?, ?, ?, ?, ?, ?, ?)"
                            proj-id (str "Project " i) (str "Description " i)
                            (name (nth [:status/planning :status/active :status/on-hold] (mod i 3)))
                            (* 100000.00 (inc i)) (nth dept-ids (quot i 2)) (nth emp-ids (* (quot i 2) 10))]))]
    {:addr-ids addr-ids :org-ids org-ids :dept-ids dept-ids :emp-ids emp-ids :proj-ids proj-ids}))

(defn run-pg2-benchmarks
  "Run read benchmarks with pg2 driver."
  []
  (let [schema-name (str "test_pg2_" (System/currentTimeMillis))
        jdbc-ds (jdbc/get-datasource test-db-config)
        jdbc-conn (jdbc/get-connection jdbc-ds)
        pg2-pool* (atom nil)]
    (try
      ;; Setup schema using JDBC
      (jdbc/execute! jdbc-conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! jdbc-conn [(str "SET search_path TO " schema-name)])
      (doseq [s (mig/automatic-schema :perf perf-attrs/all-attributes)]
        (jdbc/execute! jdbc-conn [s]))

      ;; Seed data
      (let [{:keys [org-ids dept-ids emp-ids]} (seed-basic-data! jdbc-conn)
            _ (println "Data seeded, setting up pg2 pathom env...")

            ;; Create pg2 pool with schema in pg-params (string keys required)
            pg2-pool (pg2/create-pool! {:pg2/config {:host "localhost"
                                                     :port 5432
                                                     :user "user"
                                                     :password "password"
                                                     :database "fulcro-rad-pg2"
                                                     :pg-params {"search_path" schema-name}}})
            _ (reset! pg2-pool* pg2-pool)

            ;; Build pathom env with pg2 - pool used directly, no wrapper needed
            resolvers (read/generate-resolvers perf-attrs/all-attributes :perf)
            pathom-env (-> (pci/register resolvers)
                           (assoc ::attr/key->attribute key->attribute
                                  ::rad.sql/connection-pools {:perf pg2-pool}))
            pathom-query #(p.eql/process pathom-env %)
            results (atom {})]

        (bench/print-header "PG2 READ BENCHMARKS - Pathom3 Resolvers")

        (println "Simple queries:")
        (swap! results assoc :simple-by-id
               (bench/benchmark! "Single entity by ID"
                                 #(pathom-query [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name :organization/active]}])
                                 {:iterations 50 :warmup 5}))

        (swap! results assoc :simple-scalar-only
               (bench/benchmark! "Scalar fields only"
                                 #(pathom-query [{[:employee/id (first emp-ids)]
                                                  [:employee/id :employee/first-name :employee/last-name
                                                   :employee/email :employee/active]}])
                                 {:iterations 50 :warmup 5}))

        (println "\nMedium queries:")
        (swap! results assoc :medium-to-one
               (bench/benchmark! "To-one join"
                                 #(pathom-query [{[:employee/id (first emp-ids)]
                                                  [:employee/id :employee/first-name
                                                   {:employee/department [:department/id :department/name]}]}])
                                 {:iterations 50 :warmup 5}))

        (swap! results assoc :medium-to-many-small
               (bench/benchmark! "To-many join small"
                                 #(pathom-query [{[:department/id (first dept-ids)]
                                                  [:department/id :department/name
                                                   {:department/employees [:employee/id :employee/first-name]}]}])
                                 {:iterations 50 :warmup 5}))

        (println "\nComplex queries:")
        (swap! results assoc :complex-2-level
               (bench/benchmark! "2-level deep"
                                 #(pathom-query [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name
                                                   {:organization/departments
                                                    [:department/id :department/name :department/code]}]}])
                                 {:iterations 50 :warmup 5}))

        (swap! results assoc :complex-3-level
               (bench/benchmark! "3-level deep"
                                 #(pathom-query [{[:organization/id (first org-ids)]
                                                  [:organization/id :organization/name
                                                   {:organization/departments
                                                    [:department/id :department/name
                                                     {:department/projects
                                                      [:project/id :project/name :project/status]}]}]}])
                                 {:iterations 50 :warmup 5}))

        (println "\n=== PG2 RESULTS ===")
        (pprint @results)
        @results)

      (finally
        (jdbc/execute! jdbc-conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close jdbc-conn)
        (when-let [p @pg2-pool*]
          (pg.pool/close p))))))
