(ns com.fulcrologic.rad.database-adapters.sql.value-transformer-test
  "Unit and integration tests for model->sql-value and sql->model-value transformers.

   These options allow custom value transformation when reading/writing to the database:
   - model->sql-value: (fn [clj-value] sql-value) - transform on write
   - sql->model-value: (fn [sql-value] clj-value) - transform on read

   NOTE: The keyword names in sql_options.cljc are intentionally swapped from the
   underlying RAD keys (see docstrings in sql_options.cljc for details)."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2]
   [com.fulcrologic.rad.database-adapters.sql.read :as read]
   [com.fulcrologic.rad.database-adapters.sql.write :as write]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.error :as p.error]
   [com.wsscode.pathom3.interface.eql :as p.eql]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [pg.pool :as pg.pool]
   [taoensso.encore :as enc])
  (:import
   [java.util Base64]))

;; =============================================================================
;; Custom Transformers for Testing
;; =============================================================================

(defn encrypt-value
  "Simple 'encryption' for testing (just base64 encode with prefix)."
  [value]
  (when value
    (str "ENC:" (.encodeToString (Base64/getEncoder) (.getBytes (str value) "UTF-8")))))

(defn decrypt-value
  "Simple 'decryption' for testing (decode base64 with prefix)."
  [value]
  (when (and value (str/starts-with? value "ENC:"))
    (String. (.decode (Base64/getDecoder) (subs value 4)) "UTF-8")))

(defn serialize-tags
  "Convert a vector of keywords to a comma-separated string."
  [tags]
  (when tags
    (->> tags
         (map name)
         (str/join ","))))

(defn deserialize-tags
  "Convert a comma-separated string to a vector of keywords."
  [s]
  (when (and s (not (str/blank? s)))
    (mapv keyword (str/split s #","))))

(defn json-encode
  "Simple JSON-like encoding for maps."
  [m]
  (when m
    (pr-str m)))

(defn json-decode
  "Simple JSON-like decoding for maps."
  [s]
  (when s
    (read-string s)))

;; =============================================================================
;; Test Attributes with Custom Transformers
;; =============================================================================

(def entity-id
  {::attr/qualified-key :entity/id
   ::attr/type :uuid
   ::attr/identity? true
   ::attr/schema :production
   ::rad.sql/table "entities"})

(def entity-name
  {::attr/qualified-key :entity/name
   ::attr/type :string
   ::attr/schema :production
   ::attr/identities #{:entity/id}})

;; Attribute with custom write transformer (model->sql)
;; Stored encrypted in database
(def entity-secret
  {::attr/qualified-key :entity/secret
   ::attr/type :string
   ::attr/schema :production
   ::attr/identities #{:entity/id}
   ;; NOTE: The actual RAD key is ::rad.sql/form->sql-value (write direction)
   ::rad.sql/form->sql-value encrypt-value})

;; Attribute with custom read transformer (sql->model)
;; Note: For this test we'll handle the round-trip differently
(def entity-tags
  {::attr/qualified-key :entity/tags
   ::attr/type :string
   ::attr/schema :production
   ::attr/identities #{:entity/id}
   ;; Write: vector -> comma-separated string
   ::rad.sql/form->sql-value serialize-tags
   ;; Read: comma-separated string -> vector (not currently used by write, only read)
   ::rad.sql/sql->form-value deserialize-tags})

;; Attribute with both transformers for complex data
(def entity-metadata
  {::attr/qualified-key :entity/metadata
   ::attr/type :string
   ::attr/schema :production
   ::attr/identities #{:entity/id}
   ::rad.sql/form->sql-value json-encode
   ::rad.sql/sql->form-value json-decode})

;; Attribute with write transformer that handles nil
(def entity-optional
  {::attr/qualified-key :entity/optional
   ::attr/type :string
   ::attr/schema :production
   ::attr/identities #{:entity/id}
   ::rad.sql/form->sql-value (fn [v] (if v (str/upper-case v) nil))})

(def test-attributes
  [entity-id entity-name entity-secret entity-tags entity-metadata entity-optional])

(def key->attribute (enc/keys-by ::attr/qualified-key test-attributes))

;; =============================================================================
;; Unit Tests: form->sql-value Function
;; =============================================================================

(deftest form->sql-value-with-custom-transformer
  (testing "form->sql-value applies custom transformer when present"
    (let [result (write/form->sql-value entity-secret "my-secret")]
      (is (str/starts-with? result "ENC:") "Should apply encryption")
      (is (= "my-secret" (decrypt-value result)) "Should be reversible")))

  (testing "form->sql-value handles nil value"
    (let [result (write/form->sql-value entity-secret nil)]
      (is (nil? result) "Nil input should produce nil output")))

  (testing "form->sql-value without transformer uses pg2 encoding"
    (let [result (write/form->sql-value entity-name "plain text")]
      (is (= "plain text" result) "No transformer should return value as-is"))))

(deftest form->sql-value-with-tags-transformer
  (testing "Tags transformer converts vector to comma-separated string"
    (let [result (write/form->sql-value entity-tags [:foo :bar :baz])]
      (is (= "foo,bar,baz" result))))

  (testing "Tags transformer handles empty vector"
    (let [result (write/form->sql-value entity-tags [])]
      ;; serialize-tags returns empty string for empty vector
      (is (= "" result) "Empty vector produces empty string")))

  (testing "Tags transformer handles single element"
    (let [result (write/form->sql-value entity-tags [:single])]
      (is (= "single" result)))))

(deftest form->sql-value-with-json-transformer
  (testing "JSON transformer encodes maps"
    (let [result (write/form->sql-value entity-metadata {:key "value" :count 42})]
      (is (string? result))
      (is (= {:key "value" :count 42} (json-decode result)))))

  (testing "JSON transformer handles nested structures"
    (let [nested {:outer {:inner {:deep "value"} :list [1 2 3]}}
          result (write/form->sql-value entity-metadata nested)]
      (is (= nested (json-decode result))))))

(deftest form->sql-value-with-ref-extraction
  (testing "Ref values extract the ID from ident"
    (let [ref-attr {::attr/qualified-key :foo/bar
                    ::attr/type :ref
                    ::attr/cardinality :one
                    ::attr/target :target/id}
          result (write/form->sql-value ref-attr [:target/id #uuid "abc00000-0000-0000-0000-000000000001"])]
      (is (= #uuid "abc00000-0000-0000-0000-000000000001" result)
          "Should extract ID from ident"))))

;; =============================================================================
;; Unit Tests: pg2 encode/decode Functions
;; =============================================================================

(deftest pg2-encode-for-sql-built-in-types
  (testing "Keywords are encoded as strings with colon prefix"
    (is (= ":state/AZ" (pg2/encode-for-sql :keyword :state/AZ)))
    (is (= ":foo" (pg2/encode-for-sql :keyword :foo))))

  (testing "Symbols are encoded as strings"
    (is (= "my-symbol" (pg2/encode-for-sql :symbol 'my-symbol)))
    (is (= "ns/sym" (pg2/encode-for-sql :symbol 'ns/sym))))

  (testing "Enums are encoded like keywords"
    (is (= ":status/active" (pg2/encode-for-sql :enum :status/active))))

  (testing "Unknown types pass through unchanged"
    (is (= "plain string" (pg2/encode-for-sql :string "plain string")))
    (is (= 42 (pg2/encode-for-sql :int 42)))
    (is (= 3.14 (pg2/encode-for-sql :decimal 3.14))))

  (testing "Nil values return nil"
    (is (nil? (pg2/encode-for-sql :keyword nil)))
    (is (nil? (pg2/encode-for-sql :symbol nil)))))

(deftest pg2-decode-from-sql-built-in-types
  (testing "Keyword strings are decoded to keywords"
    (is (= :state/AZ (pg2/decode-from-sql :keyword ":state/AZ")))
    (is (= :foo (pg2/decode-from-sql :keyword ":foo")))
    ;; Without colon prefix
    (is (= :bare (pg2/decode-from-sql :keyword "bare"))))

  (testing "Symbol strings are decoded to symbols"
    (is (= 'my-symbol (pg2/decode-from-sql :symbol "my-symbol")))
    (is (= 'ns/sym (pg2/decode-from-sql :symbol "ns/sym"))))

  (testing "Enums are decoded like keywords"
    (is (= :status/active (pg2/decode-from-sql :enum ":status/active"))))

  (testing "Unknown types pass through unchanged"
    (is (= "plain string" (pg2/decode-from-sql :string "plain string")))
    (is (= 42 (pg2/decode-from-sql :int 42))))

  (testing "Nil values return nil"
    (is (nil? (pg2/decode-from-sql :keyword nil)))
    (is (nil? (pg2/decode-from-sql :symbol nil)))))

(deftest pg2-instant-encoding
  (testing "Instants are converted to Timestamps"
    (let [instant (java.time.Instant/parse "2024-01-15T12:00:00Z")
          result (pg2/encode-for-sql :instant instant)]
      (is (instance? java.sql.Timestamp result))
      (is (= instant (.toInstant result)))))

  (testing "Timestamps pass through unchanged"
    (let [ts (java.sql.Timestamp/from (java.time.Instant/now))
          result (pg2/encode-for-sql :instant ts)]
      (is (identical? ts result)))))

(deftest pg2-instant-decoding
  (testing "OffsetDateTime is converted to Instant"
    (let [odt (java.time.OffsetDateTime/parse "2024-01-15T12:00:00Z")
          result (pg2/decode-from-sql :instant odt)]
      (is (instance? java.time.Instant result))))

  (testing "Timestamps are converted to Instant"
    (let [ts (java.sql.Timestamp/from (java.time.Instant/now))
          result (pg2/decode-from-sql :instant ts)]
      (is (instance? java.time.Instant result)))))

;; =============================================================================
;; Unit Tests: Row Transformer Compilation
;; =============================================================================

(deftest compile-pg2-row-transformer
  (testing "Compiled transformer applies type-specific decoders"
    (let [config {:status {:output-path [:entity/status]
                           :type :enum}
                  :name {:output-path [:entity/name]
                         :type :string}}
          transform-fn (pg2/compile-pg2-row-transformer config)
          raw-row {:status ":status/active" :name "Test"}
          result (transform-fn raw-row)]
      (is (= :status/active (:entity/status result)) "Enum should be decoded")
      (is (= "Test" (:entity/name result)) "String should pass through")))

  (testing "Compiled transformer handles nested output paths"
    (let [config {:target_id {:output-path [:entity/ref :target/id]
                              :type :uuid}}
          transform-fn (pg2/compile-pg2-row-transformer config)
          raw-row {:target_id #uuid "abc00000-0000-0000-0000-000000000001"}
          result (transform-fn raw-row)]
      (is (= #uuid "abc00000-0000-0000-0000-000000000001"
             (get-in result [:entity/ref :target/id])))))

  (testing "Compiled transformer handles nil values"
    (let [config {:status {:output-path [:entity/status]
                           :type :enum}}
          transform-fn (pg2/compile-pg2-row-transformer config)
          raw-row {:status nil}
          result (transform-fn raw-row)]
      (is (not (contains? result :entity/status)) "Nil values should not be included"))))

;; =============================================================================
;; Integration Tests: Round-trip Persistence
;; =============================================================================

(def jdbc-config
  {:jdbcUrl "jdbc:postgresql://localhost:5432/fulcro-rad-pg2?user=user&password=password"})

(def pg2-config
  {:host "localhost"
   :port 5432
   :user "user"
   :password "password"
   :database "fulcro-rad-pg2"})

(def jdbc-opts {:builder-fn rs/as-unqualified-lower-maps})

(defn generate-test-schema-name []
  (str "test_transformers_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn- split-sql-statements [sql]
  (->> (str/split sql #";\n")
       (map str/trim)
       (remove empty?)))

(defn with-test-db
  "Run function with isolated test database."
  [f]
  (let [ds (jdbc/get-datasource jdbc-config)
        schema-name (generate-test-schema-name)
        jdbc-conn (jdbc/get-connection ds)
        pg2-pool (pg.pool/pool (assoc pg2-config
                                      :pg-params {"search_path" schema-name}
                                      :pool-min-size 1
                                      :pool-max-size 2))]
    (try
      ;; Create schema and tables
      (jdbc/execute! jdbc-conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! jdbc-conn [(str "SET search_path TO " schema-name)])
      (doseq [stmt-block (mig/automatic-schema :production test-attributes)
              stmt (split-sql-statements stmt-block)]
        (jdbc/execute! jdbc-conn [stmt]))

      ;; Create combined env
      (let [resolvers (read/generate-resolvers test-attributes :production)
            env (-> (pci/register resolvers)
                    (assoc ::attr/key->attribute key->attribute
                           ::rad.sql/connection-pools {:production pg2-pool}
                           ::p.error/lenient-mode? true))]
        (f jdbc-conn env))

      (finally
        (pg.pool/close pg2-pool)
        (jdbc/execute! jdbc-conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close jdbc-conn)))))

(defn pathom-query [env query]
  (p.eql/process env query))

(defn save! [env delta]
  (write/save-form! env {::rad.form/delta delta}))

(defn get-raw-entity [conn id]
  (first (jdbc/execute! conn ["SELECT * FROM entities WHERE id = ?" id] jdbc-opts)))

(deftest ^:integration custom-write-transformer-encryption
  (testing "Custom write transformer encrypts value in database"
    (with-test-db
      (fn [conn env]
        (let [tempid (tempid/tempid)
              delta {[:entity/id tempid]
                     {:entity/name {:after "Test Entity"}
                      :entity/secret {:after "my-secret-value"}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)
              raw (get-raw-entity conn real-id)]

          ;; Verify raw DB value is encrypted
          (is (some? raw) "Entity should exist")
          (is (str/starts-with? (:secret raw) "ENC:") "Secret should be encrypted in DB")
          (is (= "my-secret-value" (decrypt-value (:secret raw)))
              "Encrypted value should decrypt correctly"))))))

(deftest ^:integration custom-write-transformer-tags
  (testing "Custom write transformer serializes tags to comma-separated string"
    (with-test-db
      (fn [conn env]
        (let [tempid (tempid/tempid)
              delta {[:entity/id tempid]
                     {:entity/name {:after "Tagged Entity"}
                      :entity/tags {:after [:red :green :blue]}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)
              raw (get-raw-entity conn real-id)]

          ;; Verify raw DB value is comma-separated
          (is (= "red,green,blue" (:tags raw)) "Tags should be comma-separated in DB"))))))

(deftest ^:integration custom-write-transformer-json-metadata
  (testing "Custom write transformer encodes map as JSON-like string"
    (with-test-db
      (fn [conn env]
        (let [tempid (tempid/tempid)
              metadata {:author "John" :version 42 :nested {:a 1}}
              delta {[:entity/id tempid]
                     {:entity/name {:after "Metadata Entity"}
                      :entity/metadata {:after metadata}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)
              raw (get-raw-entity conn real-id)]

          ;; Verify raw DB value is encoded
          (is (string? (:metadata raw)) "Metadata should be string in DB")
          (is (= metadata (json-decode (:metadata raw)))
              "Metadata should decode back to original map"))))))

(deftest ^:integration custom-transformer-handles-nil-on-write
  (testing "Custom write transformer handles nil values correctly"
    (with-test-db
      (fn [conn env]
        (let [tempid (tempid/tempid)
              delta {[:entity/id tempid]
                     {:entity/name {:after "Nil Test"}
                      :entity/secret {:after nil}
                      :entity/tags {:after nil}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)
              raw (get-raw-entity conn real-id)]

          (is (nil? (:secret raw)) "Nil secret should stay nil")
          (is (nil? (:tags raw)) "Nil tags should stay nil"))))))

(deftest ^:integration custom-transformer-update-value
  (testing "Custom write transformer is applied on update as well"
    (with-test-db
      (fn [conn env]
        ;; Create entity
        (let [tempid (tempid/tempid)
              delta {[:entity/id tempid]
                     {:entity/name {:after "Update Test"}
                      :entity/secret {:after "original"}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)]

          ;; Verify original value
          (let [raw (get-raw-entity conn real-id)]
            (is (= "original" (decrypt-value (:secret raw)))))

          ;; Update the secret
          (let [update-delta {[:entity/id real-id]
                              {:entity/secret {:before "original"
                                               :after "updated-secret"}}}]
            (save! env update-delta))

          ;; Verify updated value is encrypted
          (let [raw (get-raw-entity conn real-id)]
            (is (str/starts-with? (:secret raw) "ENC:"))
            (is (= "updated-secret" (decrypt-value (:secret raw))))))))))

(deftest ^:integration optional-transformer-uppercases
  (testing "Optional field transformer uppercases non-nil values"
    (with-test-db
      (fn [conn env]
        (let [tempid (tempid/tempid)
              delta {[:entity/id tempid]
                     {:entity/name {:after "Optional Test"}
                      :entity/optional {:after "lowercase"}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)
              raw (get-raw-entity conn real-id)]

          (is (= "LOWERCASE" (:optional raw)) "Value should be uppercased"))))))

(deftest ^:integration transformer-special-characters
  (testing "Transformers handle special characters correctly"
    (with-test-db
      (fn [conn env]
        (let [tempid (tempid/tempid)
              special-string "Secret with 'quotes', \"doubles\", and\nnewlines!"
              delta {[:entity/id tempid]
                     {:entity/name {:after "Special Chars"}
                      :entity/secret {:after special-string}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)
              raw (get-raw-entity conn real-id)]

          (is (= special-string (decrypt-value (:secret raw)))
              "Special characters should survive encryption round-trip"))))))

(deftest ^:integration transformer-unicode
  (testing "Transformers handle unicode correctly"
    (with-test-db
      (fn [conn env]
        (let [tempid (tempid/tempid)
              unicode-string "Unicöde: 日本語, 한국어, 🎉"
              delta {[:entity/id tempid]
                     {:entity/name {:after "Unicode Test"}
                      :entity/secret {:after unicode-string}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)
              raw (get-raw-entity conn real-id)]

          (is (= unicode-string (decrypt-value (:secret raw)))
              "Unicode should survive encryption round-trip"))))))

(deftest ^:integration transformer-empty-string
  (testing "Transformers handle empty strings correctly"
    (with-test-db
      (fn [conn env]
        (let [tempid (tempid/tempid)
              delta {[:entity/id tempid]
                     {:entity/name {:after "Empty String Test"}
                      :entity/secret {:after ""}}}
              result (save! env delta)
              real-id (get (:tempids result) tempid)
              raw (get-raw-entity conn real-id)]

          ;; Our encrypt-value returns "ENC:<empty-base64>" for empty string
          (is (str/starts-with? (:secret raw) "ENC:"))
          (is (= "" (decrypt-value (:secret raw)))
              "Empty string should survive encryption round-trip"))))))
