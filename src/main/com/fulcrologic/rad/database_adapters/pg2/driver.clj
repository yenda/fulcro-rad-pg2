(ns com.fulcrologic.rad.database-adapters.pg2.driver
  "PG2 driver adapter for PostgreSQL.

   PG2 is a native PostgreSQL driver that bypasses JDBC for significantly
   better performance (typically 70-99% faster than next.jdbc).

   This module provides:
   - Connection pool management
   - Query execution with prepared statement optimization
   - Type transformation (Clojure ↔ PostgreSQL)
   - Constraint handling for transactions

   Usage:
   ```clojure
   :com.fulcrologic.rad.database-adapters.pg2/databases
   {:main {:pg2/schema :production
           :pg2/config {:host \"localhost\"
                        :port 5432
                        :user \"myuser\"
                        :password \"mypassword\"
                        :database \"mydb\"}
           :pg2/pool {:pool-min-size 2
                      :pool-max-size 10}}}
   ```"
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string :as str]
   [malli.experimental :as mx]
   [pg.core :as pg]
   [pg.pool :as pool]
   [taoensso.timbre :as log])
  (:import
   [java.sql Timestamp]
   [java.time Instant OffsetDateTime]))

;; =============================================================================
;; Type Transformation (Clojure ↔ PostgreSQL)
;; =============================================================================

(defn keyword->sql-string
  "Convert keyword to string, preserving the leading colon.
   (str :state/AZ) -> \":state/AZ\""
  [x]
  (if (keyword? x) (str x) x))

(defn sql-string->keyword
  "Convert string to keyword, handling the leading colon.
   \":state/AZ\" -> :state/AZ"
  [x]
  (if (string? x)
    (if (.startsWith ^String x ":")
      (keyword (subs x 1))
      (keyword x))
    x))

(defn symbol->sql-string
  "Convert symbol to string for SQL storage."
  [x]
  (if (symbol? x) (str x) x))

(defn sql-string->symbol
  "Convert string to symbol."
  [x]
  (if (string? x) (symbol x) x))

(defn instant->timestamp
  "Convert java.time.Instant to java.sql.Timestamp for SQL storage."
  [x]
  (cond
    (instance? Instant x) (Timestamp/from x)
    (instance? Timestamp x) x
    :else x))

(defn timestamp->instant
  "Convert java.sql.Timestamp or OffsetDateTime to java.time.Instant."
  [x]
  (cond
    (instance? OffsetDateTime x) (.toInstant ^OffsetDateTime x)
    (instance? Timestamp x) (.toInstant ^Timestamp x)
    (instance? Instant x) x
    :else x))

;; Encoder/Decoder registries for extensibility
(defonce ^{:doc "Map of RAD type -> encoder function (Clojure -> SQL)"}
  encoders
  (atom {:enum keyword->sql-string
         :keyword keyword->sql-string
         :symbol symbol->sql-string
         :instant instant->timestamp}))

(defonce ^{:doc "Map of RAD type -> decoder function (SQL -> Clojure)"}
  decoders
  (atom {:enum sql-string->keyword
         :keyword sql-string->keyword
         :symbol sql-string->symbol
         :instant timestamp->instant}))

(defn register-encoder!
  "Register a custom encoder for a RAD type."
  [rad-type encoder-fn]
  (swap! encoders assoc rad-type encoder-fn))

(defn register-decoder!
  "Register a custom decoder for a RAD type."
  [rad-type decoder-fn]
  (swap! decoders assoc rad-type decoder-fn))

(mx/defn encode-for-sql :- [:maybe :any]
  "Transform Clojure value for SQL storage based on RAD type."
  [rad-type :- :keyword
   value :- [:maybe :any]]
  (when (some? value)
    (if-let [encoder (get @encoders rad-type)]
      (encoder value)
      value)))

(mx/defn decode-from-sql :- [:maybe :any]
  "Transform SQL value to Clojure value based on RAD type."
  [rad-type :- :keyword
   value :- [:maybe :any]]
  (when (some? value)
    (if-let [decoder (get @decoders rad-type)]
      (decoder value)
      value)))

(defn get-decoder
  "Get the decoder function for a RAD type. Returns nil if no decoding needed.
   Called at generation time to pre-resolve decoders.

   NOTE: This deliberately duplicates the mappings from the `decoders` atom
   as a performance optimization. Decoders are resolved once when resolvers
   are generated, avoiding atom deref + map lookup on every row at runtime."
  [rad-type]
  (case rad-type
    (:keyword :enum) sql-string->keyword
    :symbol sql-string->symbol
    :instant timestamp->instant
    nil))

(defn compile-pg2-row-transformer
  "Compile a function that transforms pg2 raw output directly to resolver format.

   This is the zero-copy fast path for pg2. Combines column name mapping
   and type transformation in a single pass.

   Optimizations applied at compile time:
   - Decoder functions pre-resolved (no case statement at runtime)
   - Custom sql->form-value transformers pre-resolved
   - Single-key paths use direct assoc instead of assoc-in

   Arguments:
     column-config - Map of database column keywords to config:
                     {:created_at {:output-path [:message/created-at]
                                   :type :instant}
                      :status     {:output-path [:message/status]
                                   :type :enum}
                      :permissions {:output-path [:token/permissions]
                                    :type :string
                                    :sql->form-value json-decode}}"
  [column-config]
  ;; Pre-process: resolve decoders and mark simple paths
  (let [entries (mapv (fn [[col {:keys [output-path type sql->form-value]}]]
                        {:col col
                         :output-path output-path
                         :output-key (first output-path)
                         :simple? (= 1 (count output-path))
                         ;; Custom transformer takes precedence over type-based decoder
                         :decoder (or sql->form-value (get-decoder type))})
                      column-config)]
    (fn transform-row [raw-row]
      (reduce
       (fn [acc {:keys [col output-path output-key simple? decoder]}]
         (if (contains? raw-row col)
           (let [v (get raw-row col)
                 decoded (if (and v decoder) (decoder v) v)]
             ;; Skip nil values - don't include them in output
             (if (some? decoded)
               (if simple?
                 (assoc acc output-key decoded)
                 (assoc-in acc output-path decoded))
               acc))
           acc))
       {}
       entries))))

;; =============================================================================
;; Value Normalization (for non-zero-copy path)
;; =============================================================================

(defn- normalize-value
  "Normalize pg2 result values to match next.jdbc conventions."
  [v]
  (cond
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
  "Convert ? placeholders to $1, $2, etc. for pg2.
   Migration helper for converting JDBC-style SQL to pg2 format.
   For new code, use pgh/format which generates $1, $2... directly."
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
                                    (let [[k v] (str/split pair #"=")]
                                      [(keyword k) v])))
                             (str/split q #"&")))]
    {:host host
     :port port
     :database database
     :user (:user query-params)
     :password (:password query-params)}))

(defn build-pool-config
  "Build pg2 pool configuration from database config."
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

(mx/defn create-pool! :- :some
  "Create a pg2 connection pool from config."
  [config :- :map]
  (let [pool-config (build-pool-config config)]
    (log/info "Creating pg2 connection pool" {:host (:host pool-config)
                                              :port (:port pool-config)
                                              :database (:database pool-config)})
    (pool/pool pool-config)))

(mx/defn close-pool! :- :nil
  "Close a pg2 connection pool."
  [pool :- :some]
  (pool/close pool))

(mx/defn stop-connection-pools! :- :nil
  "Stop all connection pools."
  [connection-pools :- [:map-of :keyword :some]]
  (doseq [[k pool] connection-pools]
    (log/info "Shutting down pool" k)
    (close-pool! pool)))

;; =============================================================================
;; Performance Logging
;; =============================================================================

(defmacro timer
  "Log query execution time. Warns if query takes > 1 second."
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

;; =============================================================================
;; Query Execution
;; =============================================================================

(mx/defn pg2-query! :- [:vector :map]
  "Execute a SQL query using pg2.
   Takes a pg2 pool and a query vector [sql & params].
   SQL must use $1, $2... placeholders (not ?). Use pgh/format for HoneySQL maps.
   Returns a vector of maps with kebab-case keyword keys."
  [pool :- :some
   query :- [:cat :string [:* :any]]]
  (let [[sql & params] query]
    (pool/with-conn [conn pool]
      (let [result (pg/execute conn sql {:params (vec params)})]
        (mapv normalize-row result)))))

(mx/defn pg2-query-raw! :- [:vector :map]
  "Execute a SQL query using pg2, returning raw results.
   Returns pg2's native format without normalization.
   SQL must use $1, $2... placeholders (not ?). Use pgh/format for HoneySQL maps.
   Use with compiled row transformers for maximum performance."
  [pool :- :some
   query :- [:cat :string [:* :any]]]
  (let [[sql & params] query]
    (pool/with-conn [conn pool]
      (pg/execute conn sql {:params (vec params)}))))

(mx/defn pg2-query-prepared! :- [:vector :map]
  "Execute a pre-compiled SQL query with array parameter.
   Uses $1::type[] syntax for 100% prepared statement cache hit rate."
  [pool :- :some
   sql :- :string
   ids :- [:sequential :any]]
  (pool/with-conn [conn pool]
    (pg/execute conn sql {:params [(vec ids)]})))

;; =============================================================================
;; Constraint Handling (PostgreSQL-specific)
;; =============================================================================

(defn relax-constraints!
  "Defer constraint checking until end of transaction.
   Call within a pg/with-transaction block."
  [conn]
  (pg/execute conn "SET CONSTRAINTS ALL DEFERRED"))

(defn add-referential-column-statement
  "Generate ALTER TABLE statement to add a FK column."
  [origin-table origin-column target-type target-table target-column]
  (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s) DEFERRABLE INITIALLY DEFERRED"
          origin-table origin-column target-type target-table target-column))
