(ns com.fulcrologic.rad.database-adapters.sql.pg2
  "PG2 driver adapter for PostgreSQL.

   PG2 is a native PostgreSQL driver that bypasses JDBC for significantly
   better performance (typically 70-99% faster than next.jdbc).

   Usage:
   Configure your database with `:sql/driver :pg2`:

   ```clojure
   :com.fulcrologic.rad.database-adapters.sql/databases
   {:main {:sql/driver :pg2
           :pg2/config {:host \"localhost\"
                        :port 5432
                        :user \"myuser\"
                        :password \"mypassword\"
                        :database \"mydb\"}
           :pg2/pool {:pool-min-size 2
                      :pool-max-size 10}}}
   ```

   Or convert from existing JDBC URL:

   ```clojure
   :com.fulcrologic.rad.database-adapters.sql/databases
   {:main {:sql/driver :pg2
           :pg2/jdbc-url \"jdbc:postgresql://localhost:5432/mydb?user=myuser&password=mypassword\"}}
   ```"
  (:require
   [camel-snake-kebab.core :as csk]
   [pg.core :as pg]
   [pg.pool :as pool]
   [taoensso.timbre :as log])
  (:import
   [java.time OffsetDateTime]))

;; =============================================================================
;; Value Normalization
;; =============================================================================

(defn- normalize-value
  "Normalize pg2 result values to match next.jdbc conventions."
  [v]
  (cond
    ;; Convert OffsetDateTime to Instant for consistency
    (instance? OffsetDateTime v) (.toInstant ^OffsetDateTime v)
    :else v))

(defn- normalize-row
  "Normalize pg2 result row - convert keys to kebab-case keywords."
  [row]
  (reduce-kv
   (fn [acc k v]
     (let [new-key (csk/->kebab-case-keyword k)]
       (assoc acc new-key (normalize-value v))))
   {}
   row))

;; =============================================================================
;; SQL Parameter Conversion
;; =============================================================================

(defn convert-params
  "Convert ? placeholders to $1, $2, etc. for pg2."
  [sql]
  (loop [s sql
         idx 1]
    (if (.contains ^String s "?")
      (recur (.replaceFirst ^String s "\\?" (java.util.regex.Matcher/quoteReplacement (str "$" idx)))
             (inc idx))
      s)))

;; =============================================================================
;; Configuration
;; =============================================================================

(defn parse-jdbc-url
  "Parse a JDBC URL into pg2 config format.

   Supports: jdbc:postgresql://host:port/database?user=x&password=y"
  [jdbc-url]
  (let [url (java.net.URI. (.replace ^String jdbc-url "jdbc:" ""))
        host (.getHost url)
        port (let [p (.getPort url)] (if (pos? p) p 5432))
        database (let [path (.getPath url)]
                   (if (and path (.startsWith ^String path "/"))
                     (subs path 1)
                     path))
        query-params (when-let [q (.getQuery url)]
                       (into {}
                             (map (fn [pair]
                                    (let [[k v] (clojure.string/split pair #"=")]
                                      [(keyword k) v])))
                             (clojure.string/split q #"&")))]
    {:host host
     :port port
     :database database
     :user (:user query-params)
     :password (:password query-params)}))

(defn build-pool-config
  "Build pg2 pool configuration from database config.

   Supports either:
   - :pg2/config map with :host, :port, :user, :password, :database
   - :pg2/jdbc-url string to be parsed

   Optional :pg2/pool map for pool settings."
  [{:pg2/keys [config jdbc-url pool]}]
  (let [base-config (if jdbc-url
                      (parse-jdbc-url jdbc-url)
                      config)
        pool-defaults {:pool-min-size 2
                       :pool-max-size 10}]
    (merge pool-defaults base-config pool)))

;; =============================================================================
;; Pool Management
;; =============================================================================

(defn create-pool
  "Create a pg2 connection pool from config."
  [config]
  (let [pool-config (build-pool-config config)]
    (log/info "Creating pg2 connection pool" {:host (:host pool-config)
                                              :port (:port pool-config)
                                              :database (:database pool-config)})
    (pool/pool pool-config)))

(defn close-pool
  "Close a pg2 connection pool."
  [pool]
  (pool/close pool))

;; =============================================================================
;; Query Execution
;; =============================================================================

(defn pg2-query!
  "Execute a SQL query using pg2.

   Takes a pg2 pool and a HoneySQL-formatted query vector [sql & params].
   Returns a vector of maps with kebab-case keyword keys.

   This function is compatible with the ::rad.sql/query-fn interface."
  [pool [sql & params]]
  (pool/with-conn [conn pool]
    (let [pg2-sql (convert-params sql)
          result (pg/execute conn pg2-sql {:params (vec params)})]
      (mapv normalize-row result))))

(defn pg2-query-raw!
  "Execute a SQL query using pg2, returning raw results.

   Returns pg2's native format: vector of maps with keyword keys matching
   database column names (e.g., :created_at not :created-at).
   Values are pg2 native types (OffsetDateTime, not Instant).

   Use with compiled row transformers for maximum performance - avoids
   the overhead of normalize-row when you have a pre-compiled transformer."
  [pool [sql & params]]
  (pool/with-conn [conn pool]
    (let [pg2-sql (convert-params sql)]
      (pg/execute conn pg2-sql {:params (vec params)}))))

(defn pg2-query-prepared!
  "Execute a pre-compiled SQL query with array parameter.

   This function is optimized for prepared statement caching:
   - Takes a static SQL string (no HoneySQL formatting at query time)
   - Uses array parameter syntax ($1::type[]) for variable-length ID lists
   - Ensures 100% prepared statement cache hit rate regardless of batch size

   Arguments:
     pool       - pg2 connection pool
     sql        - Pre-compiled SQL string with $1::type[] syntax
     ids        - Collection of IDs to pass as array parameter

   Example:
     (pg2-query-prepared! pool
       \"SELECT * FROM users WHERE id = ANY($1::uuid[])\"
       [uuid1 uuid2 uuid3])"
  [pool sql ids]
  (pool/with-conn [conn pool]
    (pg/execute conn sql {:params [(vec ids)]})))
