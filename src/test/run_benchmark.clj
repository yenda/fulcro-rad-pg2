(ns run-benchmark
  (:require
   [clojure.string :as str]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.perf.attributes :as perf-attrs]
   [com.fulcrologic.rad.database-adapters.sql.perf.benchmark :as bench]
   [com.fulcrologic.rad.database-adapters.sql.write :as write]
   [com.fulcrologic.rad.form :as rad.form]
   [pg.core :as pg]
   [pg.pool :as pg.pool]
   [taoensso.encore :as enc]))

(def pg2-config
  {:host "localhost"
   :port 5432
   :user "user"
   :password "password"
   :database "fulcro-rad-pg2"})

(def key->attribute (enc/keys-by ::attr/qualified-key perf-attrs/all-attributes))

(defn make-simple-insert-delta []
  (let [tid (tempid/tempid)]
    {[:address/id tid]
     {:address/street {:after "123 Main St"}
      :address/city {:after "Test City"}
      :address/state {:after "CA"}
      :address/postal-code {:after "12345"}
      :address/country {:after "USA"}}}))

(defn make-batch-insert-delta [n]
  (into {}
        (for [i (range n)]
          (let [tid (tempid/tempid)]
            [[:address/id tid]
             {:address/street {:after (str "Street " i)}
              :address/city {:after "Batch City"}
              :address/state {:after "TX"}
              :address/postal-code {:after (format "%05d" i)}
              :address/country {:after "USA"}}]))))

(defn split-ddl [ddl-str]
  (->> (str/split ddl-str #";\n")
       (map str/trim)
       (remove str/blank?)
       (map #(str % ";"))))

(defn run-benchmarks []
  (let [schema-name (str "test_current_" (System/currentTimeMillis))
        pg2-pool (pg.pool/pool (assoc pg2-config :pg-params {"search_path" schema-name}))
        setup-pool (pg.pool/pool pg2-config)]
    (try
      ;; Setup schema
      (pg.pool/with-conn [conn setup-pool]
        (pg/execute conn (str "CREATE SCHEMA " schema-name))
        (pg/execute conn (str "SET search_path TO " schema-name))
        (doseq [ddl-block (mig/automatic-schema :perf perf-attrs/all-attributes)]
          (doseq [stmt (split-ddl ddl-block)]
            (pg/execute conn stmt))))

      (let [env {::attr/key->attribute key->attribute
                 ::rad.sql/connection-pools {:perf pg2-pool}}
            clear! #(pg.pool/with-conn [conn pg2-pool]
                      (pg/execute conn "TRUNCATE TABLE addresses CASCADE"))]

        (println "\n=== CURRENT WRITE BENCHMARKS ===\n")

        (println "Simple operations:")
        (let [r1 (bench/benchmark! "simple-insert"
                                   #(write/save-form! env {::rad.form/delta (make-simple-insert-delta)}))]
          (clear!)

          (println "\nBatch operations:")
          (let [r2 (bench/benchmark! "batch-10"
                                     #(do (write/save-form! env {::rad.form/delta (make-batch-insert-delta 10)})
                                          (clear!)))
                r3 (bench/benchmark! "batch-50"
                                     #(do (write/save-form! env {::rad.form/delta (make-batch-insert-delta 50)})
                                          (clear!)))
                r4 (bench/benchmark! "batch-100"
                                     #(do (write/save-form! env {::rad.form/delta (make-batch-insert-delta 100)})
                                          (clear!)))]

            (println "\n=== RESULTS ===")
            (println "simple-insert:" (:mean r1))
            (println "batch-10:" (:mean r2))
            (println "batch-50:" (:mean r3))
            (println "batch-100:" (:mean r4)))))

      (finally
        (pg.pool/close pg2-pool)
        (pg.pool/with-conn [conn setup-pool]
          (pg/execute conn (str "DROP SCHEMA " schema-name " CASCADE")))
        (pg.pool/close setup-pool)))))

(run-benchmarks)
