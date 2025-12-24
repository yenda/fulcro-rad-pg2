(ns com.fulcrologic.rad.database-adapters.sql.query
  "This namespace provides query builders for various concerns of
  a SQL database in a RAD application. The main concerns are:

  - Fetch queries (with joins) coming from resolvers
  - Building custom queries based of off RAD attributes
  - Persisting data based off submitted form deltas"
  (:require
   [com.fulcrologic.guardrails.core :refer [=> >defn ?]]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [jsonista.core :as j]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [taoensso.timbre :as log])
  (:import
   (java.sql ResultSet ResultSetMetaData Types)))

(defmacro timer
  [s expr params]
  `(let [start# (. System (nanoTime))
         _# (log/debug (format "Running %s" ~s) ~params)
         ret# ~expr
         duration# (/ (double (- (. System (nanoTime)) start#))
                      1000000.0)]
     (if (< 1000 duration#)
       (log/warn (format "Ran %s in %f msecs" ~s duration#) ~params)
       (log/debug (format "Ran %s in %f msecs" ~s duration#) ~params))
     ret#))

(defn RAD-column-reader
  "An example column-reader that still uses `.getObject` but expands CLOB
  columns into strings."
  [^ResultSet rs ^ResultSetMetaData md ^Integer i]
  (let [col-type (.getColumnType md i)
        col-type-name (.getColumnTypeName md i)]
    (cond
      (= col-type-name "JSON") (j/read-value (.getObject rs i))
      (= col-type Types/CLOB) (rs/clob->string (.getClob rs i))
      (#{Types/TIMESTAMP Types/TIMESTAMP_WITH_TIMEZONE} col-type) (.getTimestamp rs i)
      :else (.getObject rs i))))

;; Type coercion is handled by the row builder
(def row-builder (rs/as-maps-adapter rs/as-unqualified-kebab-maps RAD-column-reader))

(defn jdbc-query!
  "Execute a query using next.jdbc. This is the default implementation."
  [datasource query]
  (jdbc/execute! datasource query {:builder-fn row-builder}))

(defn- execute-query!
  "Execute a query using the appropriate driver.

   pool-or-wrapper can be:
   - {:driver :pg2 :pool pg2-pool} - pg2 driver
   - {:driver :hikaricp :pool hikari-datasource} - HikariCP/next.jdbc
   - raw datasource (backward compatibility)"
  [pool-or-wrapper query]
  (if (map? pool-or-wrapper)
    (let [{:keys [driver pool]} pool-or-wrapper]
      (case driver
        :pg2 (pg2/pg2-query! pool query)
        ;; Default: HikariCP/next.jdbc
        (jdbc-query! pool query)))
    ;; Backward compatibility: raw datasource
    (jdbc-query! pool-or-wrapper query)))

(>defn eql-query!
       [{::rad.sql/keys [connection-pools query-fn]}
        query
        schema
        resolver-input]
       [any? vector? keyword? coll? => (? coll?)]
       (let [pool-wrapper (or (get connection-pools schema)
                              (throw (ex-info "Data source missing for schema" {:schema schema})))
             execute-fn (or query-fn execute-query!)]
         (timer "SQL query with execute!"
                (execute-fn pool-wrapper query)
                {:query query})))
