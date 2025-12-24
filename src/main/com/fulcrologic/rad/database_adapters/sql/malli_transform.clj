(ns com.fulcrologic.rad.database-adapters.sql.malli-transform
  "Malli-based type transformers for SQL value encoding/decoding.

   This module provides bidirectional transformation between Clojure values
   and SQL values based on RAD attribute types.

   Write path (encode):
     Clojure value -> SQL value
     - :enum, :keyword -> string (with leading colon preserved)
     - :symbol -> string
     - :instant -> java.sql.Timestamp
     - others -> pass through

   Read path (decode):
     SQL value -> Clojure value
     - string (for :enum, :keyword) -> keyword
     - string (for :symbol) -> symbol
     - java.sql.Timestamp -> java.time.Instant
     - others -> pass through

   Performance Notes:
     The implementation uses lookup maps for O(1) dispatch, which is more
     extensible than case statements while maintaining equivalent performance.

     For complex nested structures, use the Malli transformer directly:
       (m/encode schema value (sql-transformer))
       (m/decode schema value (sql-transformer))

   Extensibility:
     Register custom type transformers via `register-encoder!` and
     `register-decoder!` for application-specific types."
  (:require
   [malli.core :as m]
   [malli.transform :as mt])
  (:import
   [java.sql Timestamp]
   [java.time Instant OffsetDateTime]))

;; =============================================================================
;; Transformation Functions
;; =============================================================================

(defn keyword->sql-string
  "Convert keyword to string, preserving the leading colon.
   This matches the original RAD behavior: (str :state/AZ) -> \":state/AZ\""
  [x]
  (if (keyword? x) (str x) x))

(defn sql-string->keyword
  "Convert string to keyword, handling the leading colon.
   Input \":state/AZ\" -> :state/AZ"
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
  "Convert java.sql.Timestamp to java.time.Instant."
  [x]
  (cond
    (instance? Timestamp x) (.toInstant ^Timestamp x)
    (instance? Instant x) x
    :else x))

;; =============================================================================
;; Encoder/Decoder Registries
;; =============================================================================
;; These are atoms to allow runtime extension for custom types.

(defonce ^{:doc "Map of RAD type -> encoder function (Clojure -> SQL)"}
  encoders
  (atom {:enum     keyword->sql-string
         :keyword  keyword->sql-string
         :symbol   symbol->sql-string
         :instant  instant->timestamp}))

(defonce ^{:doc "Map of RAD type -> decoder function (SQL -> Clojure)"}
  decoders
  (atom {:enum     sql-string->keyword
         :keyword  sql-string->keyword
         :symbol   sql-string->symbol
         :instant  timestamp->instant}))

(defn register-encoder!
  "Register a custom encoder for a RAD type.

   Example:
     (register-encoder! :my-type my-clj->sql-fn)"
  [rad-type encoder-fn]
  (swap! encoders assoc rad-type encoder-fn))

(defn register-decoder!
  "Register a custom decoder for a RAD type.

   Example:
     (register-decoder! :my-type my-sql->clj-fn)"
  [rad-type decoder-fn]
  (swap! decoders assoc rad-type decoder-fn))

;; =============================================================================
;; Fast Scalar API (for single values)
;; =============================================================================

(defn encode-for-sql
  "Transform Clojure value for SQL storage based on RAD type.

   This is the primary API for the write path. Uses O(1) lookup for
   type-specific transformation.

   Arguments:
     rad-type - The ::attr/type from the attribute (e.g., :enum, :keyword, :instant)
     value    - The Clojure value to transform

   Returns:
     The SQL-compatible value, or nil if value is nil."
  [rad-type value]
  (when (some? value)
    (if-let [encoder (get @encoders rad-type)]
      (encoder value)
      value)))

(defn decode-from-sql
  "Transform SQL value to Clojure value based on RAD type.

   This is the primary API for the read path. Uses O(1) lookup for
   type-specific transformation.

   Arguments:
     rad-type - The ::attr/type from the attribute (e.g., :enum, :keyword, :instant)
     value    - The SQL value to transform

   Returns:
     The Clojure value, or nil if value is nil."
  [rad-type value]
  (when (some? value)
    (if-let [decoder (get @decoders rad-type)]
      (decoder value)
      value)))

;; =============================================================================
;; Malli Transformer (for complex nested structures)
;; =============================================================================
;; Use this when you need to transform entire data structures at once,
;; e.g., for bulk operations or when working with Malli schemas directly.

(def sql-transformer
  "A Malli transformer for SQL encoding/decoding.

   Usage:
     (m/encode [:map [:status :keyword] [:created :instant]] data (sql-transformer))
     (m/decode [:map [:status :keyword] [:created :instant]] sql-data (sql-transformer))

   Note: For simple single-value transformations, prefer encode-for-sql/decode-from-sql
   as they have less overhead."
  (mt/transformer
   {:name :sql
    :encoders {:keyword keyword->sql-string
               :symbol  symbol->sql-string
               ;; For :instant, we need a custom schema since Malli's :inst is java.util.Date
               ;; Define via schema properties or use :time/instant from malli.experimental.time
               }
    :decoders {:keyword sql-string->keyword
               :symbol  sql-string->symbol}}))

;; =============================================================================
;; Malli Schema Definitions for SQL Types
;; =============================================================================
;; These can be used in schema definitions for validation + transformation.

(def SqlInstant
  "Malli schema for java.time.Instant with SQL transformation support.

   Example:
     [:map [:created-at SqlInstant]]"
  [:fn {:encode/sql instant->timestamp
        :decode/sql timestamp->instant}
   #(instance? Instant %)])

(def SqlKeyword
  "Malli schema for keywords stored as strings in SQL.

   Example:
     [:map [:status SqlKeyword]]"
  [:keyword {:encode/sql keyword->sql-string
             :decode/sql sql-string->keyword}])

(def SqlSymbol
  "Malli schema for symbols stored as strings in SQL.

   Example:
     [:map [:fn-name SqlSymbol]]"
  [:symbol {:encode/sql symbol->sql-string
            :decode/sql sql-string->symbol}])

;; =============================================================================
;; Pre-compiled Transformers (for maximum performance with known schemas)
;; =============================================================================
;; When you have a known schema that will be used repeatedly, compile the
;; encoder/decoder once and reuse:
;;
;;   (def my-encoder (m/encoder my-schema (sql-transformer)))
;;   (def my-decoder (m/decoder my-schema (sql-transformer)))
;;
;; The compiled functions have minimal overhead per call.

(defn compile-encoder
  "Compile a Malli encoder for a schema. Returns a function that transforms values.

   Example:
     (def encode-user (compile-encoder [:map [:status :keyword] [:name :string]]))
     (encode-user {:status :active :name \"Alice\"})
     ;; => {:status \":active\" :name \"Alice\"}"
  [schema]
  (m/encoder schema sql-transformer))

(defn compile-decoder
  "Compile a Malli decoder for a schema. Returns a function that transforms values.

   Example:
     (def decode-user (compile-decoder [:map [:status :keyword] [:name :string]]))
     (decode-user {:status \":active\" :name \"Alice\"})
     ;; => {:status :active :name \"Alice\"}"
  [schema]
  (m/decoder schema sql-transformer))

;; =============================================================================
;; Cached Encoder/Decoder Factory
;; =============================================================================
;; For dynamic schemas, use memoization to avoid recompilation.

(def ^:private encoder-cache
  "Memoized encoder factory for dynamic schemas."
  (memoize (fn [schema] (m/encoder schema sql-transformer))))

(def ^:private decoder-cache
  "Memoized decoder factory for dynamic schemas."
  (memoize (fn [schema] (m/decoder schema sql-transformer))))

(defn cached-encoder
  "Get or create a cached encoder for a schema.

   Use this when the schema is dynamic but may repeat:
     (let [encode (cached-encoder some-schema)]
       (encode data))"
  [schema]
  (encoder-cache schema))

(defn cached-decoder
  "Get or create a cached decoder for a schema.

   Use this when the schema is dynamic but may repeat:
     (let [decode (cached-decoder some-schema)]
       (decode sql-data))"
  [schema]
  (decoder-cache schema))

;; =============================================================================
;; RAD Attribute -> Malli Schema Generation
;; =============================================================================
;; These functions convert RAD attribute definitions to Malli schemas,
;; enabling compiled decoders for efficient row transformation.

(defn rad-type->malli-schema
  "Convert a RAD attribute type to a Malli schema with SQL decode support.

   For types that need transformation (:keyword, :enum, :symbol, :instant),
   returns a schema with :decode/sql property. Other types pass through as :any.

   Arguments:
     rad-type - The ::attr/type from an attribute (e.g., :keyword, :instant)

   Returns:
     A Malli schema, possibly with :decode/sql transformer attached."
  [rad-type]
  (case rad-type
    (:keyword :enum)
    [:any {:decode/sql sql-string->keyword}]

    :symbol
    [:any {:decode/sql sql-string->symbol}]

    :instant
    [:any {:decode/sql timestamp->instant}]

    ;; All other types pass through unchanged
    :any))

(defn attribute->column-key
  "Get the SQL column name as a keyword from a RAD attribute.

   Uses ::rad.sql/column-name if present, otherwise derives from qualified-key."
  [attr qualified-key-fn column-name-fn]
  (if-let [col-name (column-name-fn attr)]
    (keyword col-name)
    (keyword (name (qualified-key-fn attr)))))

(defn attributes->malli-schema
  "Generate a Malli map schema from a collection of RAD attributes.

   The generated schema maps SQL column names (as keywords) to appropriate
   Malli schemas based on ::attr/type. Types needing transformation get
   :decode/sql properties attached.

   Arguments:
     attributes     - Collection of RAD attribute maps
     qualified-key-fn - Function to get qualified key from attr (e.g., ::attr/qualified-key)
     column-name-fn   - Function to get column name from attr (e.g., ::rad.sql/column-name)
     type-fn          - Function to get type from attr (e.g., ::attr/type)

   Returns:
     A Malli map schema like:
       [:map
        [:id :any]
        [:status [:any {:decode/sql sql-string->keyword}]]
        [:created_at [:any {:decode/sql timestamp->instant}]]]

   Example:
     (attributes->malli-schema
       [{::attr/qualified-key :user/id, ::attr/type :uuid}
        {::attr/qualified-key :user/status, ::attr/type :keyword}]
       ::attr/qualified-key
       ::rad.sql/column-name
       ::attr/type)

   Performance:
     This function is intended to be called once at resolver generation time.
     The resulting schema should be compiled with `compile-decoder` for use
     at query time."
  [attributes qualified-key-fn column-name-fn type-fn]
  (let [entries (mapv (fn [attr]
                        (let [col-key (attribute->column-key attr qualified-key-fn column-name-fn)
                              rad-type (type-fn attr)
                              malli-schema (rad-type->malli-schema rad-type)]
                          [col-key malli-schema]))
                      attributes)]
    (into [:map {:closed false}] entries)))

(defn compile-row-decoder
  "Create a compiled decoder for transforming SQL rows based on RAD attributes.

   This is the main entry point for resolver generation. Call once per resolver
   during resolver generation, then use the returned function for each row.

   Arguments:
     attributes     - Collection of RAD attribute maps to include in schema
     qualified-key-fn - Function to get qualified key from attr
     column-name-fn   - Function to get column name from attr
     type-fn          - Function to get type from attr

   Returns:
     A function (fn [row] transformed-row) that applies all necessary
     SQL->Clojure transformations in a single pass.

   Example:
     ;; At resolver generation time (once):
     (def decode-user-row
       (compile-row-decoder user-attrs
                            ::attr/qualified-key
                            ::rad.sql/column-name
                            ::attr/type))

     ;; At query time (per row):
     (map decode-user-row sql-rows)

   Performance:
     The compiled decoder has minimal overhead per call. Malli compiles the
     schema to an efficient transformation function that applies all decoders
     in a single traversal."
  [attributes qualified-key-fn column-name-fn type-fn]
  (let [schema (attributes->malli-schema attributes qualified-key-fn column-name-fn type-fn)]
    (m/decoder schema sql-transformer)))

;; =============================================================================
;; pg2 Zero-Copy Row Transformation
;; =============================================================================
;; These functions provide direct pg2 native output -> resolver output format
;; transformation in a single pass, without intermediate key conversions.

(defn- pg2-decode-value
  "Decode a pg2 native value based on RAD type.

   pg2 returns OffsetDateTime for timestamps (not java.sql.Timestamp),
   so we handle that specifically here."
  [rad-type value]
  (when (some? value)
    (case rad-type
      (:keyword :enum)
      (sql-string->keyword value)

      :symbol
      (sql-string->symbol value)

      :instant
      (cond
        (instance? OffsetDateTime value) (.toInstant ^OffsetDateTime value)
        (instance? Timestamp value) (.toInstant ^Timestamp value)
        (instance? Instant value) value
        :else value)

      ;; All other types pass through unchanged
      value)))

(defn compile-pg2-row-transformer
  "Compile a function that transforms pg2 raw output directly to resolver format.

   This is the zero-copy fast path for pg2. It combines:
   1. Column name mapping (db column -> output path)
   2. Type transformation (pg2 types -> Clojure types)

   In a single pass over the row data.

   Arguments:
     column-config - Map of database column keywords to config:
                     {:created_at {:output-path [:message/created-at]
                                   :type :instant}
                      :status     {:output-path [:message/status]
                                   :type :enum}}

   Returns:
     A function (fn [raw-row] resolver-output-map) that transforms
     pg2's native output directly to the resolver's expected format.

   Performance:
     This avoids:
     - normalize-row's key conversion (snake_case -> kebab-case)
     - Separate decode pass for type transformations
     - Separate column-mapping pass for qualified keywords

   Example:
     ;; At resolver generation time (once):
     (def transform-row
       (compile-pg2-row-transformer
         {:id         {:output-path [:user/id] :type :uuid}
          :created_at {:output-path [:user/created-at] :type :instant}
          :status     {:output-path [:user/status] :type :enum}}))

     ;; At query time (per row):
     (mapv transform-row (pg2/pg2-query-raw! pool query))"
  [column-config]
  (let [;; Pre-compute column entries for the tight loop
        entries (vec column-config)]
    (fn transform-row [raw-row]
      (reduce
       (fn [acc [col {:keys [output-path type]}]]
         (let [v (get raw-row col)]
           (if (some? v)
             (assoc-in acc output-path (pg2-decode-value type v))
             acc)))
       {}
       entries))))
