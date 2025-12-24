(ns com.fulcrologic.rad.database-adapters.sql.connection
  "Connection pool management for pg2 PostgreSQL driver."
  (:require
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as sql.migration]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]))

(defn stop-connection-pools!
  "Stop all connection pools."
  [connection-pools]
  (doseq [[k pool] connection-pools]
    (log/info "Shutting down pool" k)
    (pg2/close-pool pool)))

(defn create-connection-pools!
  "Create pg2 connection pools for all configured databases."
  [config all-attributes]
  (enc/if-let [databases (get config ::rad.sql/databases)
               pools (reduce
                      (fn [pools [dbkey dbconfig]]
                        (log/info "Creating pg2 pool for" dbkey)
                        (assoc pools dbkey (pg2/create-pool dbconfig)))
                      {} databases)]
    (try
      (sql.migration/migrate! config all-attributes pools)
      pools
      (catch Throwable t
        (log/error "DATABASE STARTUP FAILED:" t)
        (stop-connection-pools! pools)
        (throw t)))
    (do
      (log/error "SQL Database configuration missing/incorrect.")
      (throw (ex-info "SQL Configuration failed." {})))))
