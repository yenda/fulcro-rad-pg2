(ns com.fulcrologic.rad.database-adapters.sql.connection
  (:require
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as sql.migration]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)
           (java.util Properties)))

(defn- create-hikari-pool
  "Create a HikariDataSource for connection pooling from a properties map."
  [pool-properties]
  (try
    (let [^Properties props (Properties.)]
      (doseq [[k v] pool-properties]
        (when (and k v)
          (.setProperty props k v)))
      (let [^HikariConfig config (HikariConfig. props)]
        (HikariDataSource. config)))
    (catch Exception e
      (log/error "Unable to create Hikari Datasource: " (.getMessage e)))))

(defn- create-pool
  "Create a connection pool based on driver type.

   Supported drivers:
   - :hikaricp (default) - HikariCP with next.jdbc
   - :pg2 - Native PostgreSQL driver (significantly faster)"
  [dbkey dbconfig]
  (let [driver (get dbconfig :sql/driver :hikaricp)]
    (case driver
      :pg2 (do
             (log/info (str "Creating pg2 pool for " dbkey))
             {:driver :pg2
              :pool (pg2/create-pool dbconfig)})
      ;; Default: HikariCP
      (do
        (log/info (str "Creating HikariCP pool for " dbkey))
        {:driver :hikaricp
         :pool (create-hikari-pool (:hikaricp/config dbconfig))}))))

(defn- close-pool!
  "Close a connection pool based on its driver type."
  [{:keys [driver pool]}]
  (case driver
    :pg2 (pg2/close-pool pool)
    ;; Default: HikariCP
    (.close ^HikariDataSource pool)))

(defn stop-connection-pools!
  "Stop all connection pools."
  [connection-pools]
  (doseq [[k pool-wrapper] connection-pools]
    (log/info "Shutting down pool " k)
    (close-pool! pool-wrapper)))

(defn create-connection-pools!
  "Create connection pools for all configured databases.

   Each database config can specify :sql/driver:
   - :hikaricp (default) - Uses HikariCP + next.jdbc
   - :pg2 - Uses native pg2 driver (PostgreSQL only, much faster)"
  [config all-attributes]
  (enc/if-let [databases (get config ::rad.sql/databases)
               pools (reduce
                      (fn [pools [dbkey dbconfig]]
                        (assoc pools dbkey (create-pool dbkey dbconfig)))
                      {} databases)]
    (try
      (sql.migration/migrate! config all-attributes pools)
      pools
      (catch Throwable t
        (log/error "DATABASE STARTUP FAILED: " t)
        (stop-connection-pools! pools)
        (throw t)))
    (do
      (log/error "SQL Database configuration missing/incorrect.")
      (throw (ex-info "SQL Configuration failed." {})))))
