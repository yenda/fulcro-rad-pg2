(ns com.fulcrologic.rad.database-adapters.pg2.driver-test
  "Tests for pg2 type transformation functions."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.database-adapters.pg2.driver :as pg2])
  (:import
   [java.sql Timestamp]
   [java.time Instant OffsetDateTime]))

;; =============================================================================
;; Helper function tests
;; =============================================================================

(deftest keyword->sql-string-test
  (testing "converts keywords to strings with leading colon"
    (is (= ":foo" (pg2/keyword->sql-string :foo)))
    (is (= ":foo/bar" (pg2/keyword->sql-string :foo/bar)))
    (is (= ":state/AZ" (pg2/keyword->sql-string :state/AZ))))
  (testing "passes through non-keywords"
    (is (= "hello" (pg2/keyword->sql-string "hello")))
    (is (= 42 (pg2/keyword->sql-string 42)))
    (is (nil? (pg2/keyword->sql-string nil)))))

(deftest sql-string->keyword-test
  (testing "converts strings to keywords, handling leading colon"
    (is (= :foo (pg2/sql-string->keyword ":foo")))
    (is (= :foo/bar (pg2/sql-string->keyword ":foo/bar")))
    (is (= :state/AZ (pg2/sql-string->keyword ":state/AZ"))))
  (testing "handles strings without leading colon"
    (is (= :foo (pg2/sql-string->keyword "foo")))
    (is (= :foo/bar (pg2/sql-string->keyword "foo/bar"))))
  (testing "passes through non-strings"
    (is (= :already (pg2/sql-string->keyword :already)))
    (is (= 42 (pg2/sql-string->keyword 42)))))

(deftest symbol->sql-string-test
  (testing "converts symbols to strings"
    (is (= "foo" (pg2/symbol->sql-string 'foo)))
    (is (= "my.ns/fn" (pg2/symbol->sql-string 'my.ns/fn))))
  (testing "passes through non-symbols"
    (is (= "hello" (pg2/symbol->sql-string "hello")))
    (is (= :key (pg2/symbol->sql-string :key)))))

(deftest sql-string->symbol-test
  (testing "converts strings to symbols"
    (is (= 'foo (pg2/sql-string->symbol "foo")))
    (is (= 'my.ns/fn (pg2/sql-string->symbol "my.ns/fn"))))
  (testing "passes through non-strings"
    (is (= 'already (pg2/sql-string->symbol 'already)))))

(deftest instant->timestamp-test
  (let [now (Instant/now)]
    (testing "converts Instant to Timestamp"
      (let [result (pg2/instant->timestamp now)]
        (is (instance? Timestamp result))
        (is (= (.toEpochMilli now) (.getTime ^Timestamp result)))))
    (testing "passes through Timestamps"
      (let [ts (Timestamp/from now)]
        (is (identical? ts (pg2/instant->timestamp ts)))))
    (testing "passes through other values"
      (is (= "hello" (pg2/instant->timestamp "hello"))))))

(deftest timestamp->instant-test
  (let [now (Instant/now)
        ts (Timestamp/from now)]
    (testing "converts Timestamp to Instant"
      (let [result (pg2/timestamp->instant ts)]
        (is (instance? Instant result))
        (is (= (.getTime ts) (.toEpochMilli ^Instant result)))))
    (testing "converts OffsetDateTime to Instant"
      (let [odt (OffsetDateTime/now)
            result (pg2/timestamp->instant odt)]
        (is (instance? Instant result))))
    (testing "passes through Instants"
      (is (identical? now (pg2/timestamp->instant now))))
    (testing "passes through other values"
      (is (= "hello" (pg2/timestamp->instant "hello"))))))

;; =============================================================================
;; Public API tests
;; =============================================================================

(deftest encode-for-sql-test
  (testing "encodes keywords/enums to strings"
    (is (= ":status/active" (pg2/encode-for-sql :enum :status/active)))
    (is (= ":foo" (pg2/encode-for-sql :keyword :foo))))
  (testing "encodes symbols to strings"
    (is (= "my.ns/fn" (pg2/encode-for-sql :symbol 'my.ns/fn))))
  (testing "encodes instants to timestamps"
    (let [now (Instant/now)]
      (is (instance? Timestamp (pg2/encode-for-sql :instant now)))))
  (testing "passes through other types"
    (is (= "hello" (pg2/encode-for-sql :string "hello")))
    (is (= 42 (pg2/encode-for-sql :int 42)))
    (is (= true (pg2/encode-for-sql :boolean true))))
  (testing "returns nil for nil input"
    (is (nil? (pg2/encode-for-sql :keyword nil)))
    (is (nil? (pg2/encode-for-sql :instant nil)))))

(deftest decode-from-sql-test
  (testing "decodes strings to keywords/enums"
    (is (= :status/active (pg2/decode-from-sql :enum ":status/active")))
    (is (= :foo (pg2/decode-from-sql :keyword ":foo")))
    (is (= :foo (pg2/decode-from-sql :keyword "foo"))))
  (testing "decodes strings to symbols"
    (is (= 'my.ns/fn (pg2/decode-from-sql :symbol "my.ns/fn"))))
  (testing "decodes timestamps to instants"
    (let [ts (Timestamp/from (Instant/now))]
      (is (instance? Instant (pg2/decode-from-sql :instant ts)))))
  (testing "passes through other types"
    (is (= "hello" (pg2/decode-from-sql :string "hello")))
    (is (= 42 (pg2/decode-from-sql :int 42)))
    (is (= true (pg2/decode-from-sql :boolean true))))
  (testing "returns nil for nil input"
    (is (nil? (pg2/decode-from-sql :keyword nil)))
    (is (nil? (pg2/decode-from-sql :instant nil)))))

(deftest roundtrip-test
  (testing "encode -> decode roundtrip preserves values"
    (let [test-cases [[:keyword :my/keyword]
                      [:enum :status/active]
                      [:symbol 'my.ns/fn]
                      [:instant (Instant/now)]
                      [:string "hello"]
                      [:int 42]
                      [:boolean true]]]
      (doseq [[rad-type value] test-cases]
        (testing (str "roundtrip for " rad-type)
          (let [encoded (pg2/encode-for-sql rad-type value)
                decoded (pg2/decode-from-sql rad-type encoded)]
            (is (= value decoded))))))))

;; =============================================================================
;; Custom registration tests
;; =============================================================================

(deftest register-encoder-test
  (let [original-encoders @pg2/encoders]
    (try
      (testing "registers custom encoder"
        (pg2/register-encoder! :test-type (fn [x] (str "TEST:" x)))
        (is (= "TEST:hello" (pg2/encode-for-sql :test-type "hello"))))
      (finally
        (reset! pg2/encoders original-encoders)))))

(deftest register-decoder-test
  (let [original-decoders @pg2/decoders]
    (try
      (testing "registers custom decoder"
        (pg2/register-decoder! :test-type (fn [x] (keyword (subs x 5))))
        (is (= :hello (pg2/decode-from-sql :test-type "TEST:hello"))))
      (finally
        (reset! pg2/decoders original-decoders)))))

;; =============================================================================
;; Zero-copy row transformer tests
;; =============================================================================

(deftest compile-pg2-row-transformer-test
  (testing "transforms row based on column config"
    (let [transform (pg2/compile-pg2-row-transformer
                     {:status {:output-path [:user/status] :type :keyword}
                      :name {:output-path [:user/name] :type :string}})]
      (is (= {:user/status :active :user/name "Alice"}
             (transform {:status ":active" :name "Alice"})))))

  (testing "handles instant type"
    (let [_now (Instant/now)
          odt (OffsetDateTime/now)
          transform (pg2/compile-pg2-row-transformer
                     {:created_at {:output-path [:user/created-at] :type :instant}})]
      (is (instance? Instant (:user/created-at (transform {:created_at odt}))))))

  (testing "handles nil values"
    (let [transform (pg2/compile-pg2-row-transformer
                     {:status {:output-path [:user/status] :type :keyword}})]
      (is (= {} (transform {:status nil})))))

  (testing "handles nested output paths"
    (let [transform (pg2/compile-pg2-row-transformer
                     {:id {:output-path [:user/address :address/id] :type :uuid}})]
      (is (= {:user/address {:address/id 123}}
             (transform {:id 123}))))))

;; =============================================================================
;; SQL parameter conversion tests
;; =============================================================================

(deftest convert-params-test
  (testing "converts ? to $1, $2, etc"
    (is (= "SELECT * FROM users WHERE id = $1"
           (pg2/convert-params "SELECT * FROM users WHERE id = ?")))
    (is (= "SELECT * FROM users WHERE id = $1 AND name = $2"
           (pg2/convert-params "SELECT * FROM users WHERE id = ? AND name = ?"))))
  (testing "handles no placeholders"
    (is (= "SELECT * FROM users"
           (pg2/convert-params "SELECT * FROM users")))))

;; =============================================================================
;; JDBC URL parsing tests
;; =============================================================================

(deftest parse-jdbc-url-test
  (testing "parses standard JDBC URL with all components"
    (let [result (pg2/parse-jdbc-url "jdbc:postgresql://localhost:5432/mydb?user=admin&password=secret")]
      (is (= "localhost" (:host result)))
      (is (= 5432 (:port result)))
      (is (= "mydb" (:database result)))
      (is (= "admin" (:user result)))
      (is (= "secret" (:password result)))))

  (testing "parses URL with non-standard port"
    (let [result (pg2/parse-jdbc-url "jdbc:postgresql://db.example.com:5433/production?user=app&password=pass123")]
      (is (= "db.example.com" (:host result)))
      (is (= 5433 (:port result)))
      (is (= "production" (:database result)))))

  (testing "defaults to port 5432 when not specified"
    (let [result (pg2/parse-jdbc-url "jdbc:postgresql://localhost/testdb?user=test&password=test")]
      (is (= "localhost" (:host result)))
      (is (= 5432 (:port result)))
      (is (= "testdb" (:database result)))))

  (testing "handles URL without query parameters"
    (let [result (pg2/parse-jdbc-url "jdbc:postgresql://localhost:5432/mydb")]
      (is (= "localhost" (:host result)))
      (is (= 5432 (:port result)))
      (is (= "mydb" (:database result)))
      (is (nil? (:user result)))
      (is (nil? (:password result)))))

  (testing "handles IP address as host"
    (let [result (pg2/parse-jdbc-url "jdbc:postgresql://192.168.1.100:5432/db?user=u&password=p")]
      (is (= "192.168.1.100" (:host result)))
      (is (= 5432 (:port result)))))

  (testing "handles special characters in password"
    ;; Note: URL-encoded characters get decoded by java.net.URI
    (let [result (pg2/parse-jdbc-url "jdbc:postgresql://localhost:5432/db?user=admin&password=p%40ss")]
      (is (= "admin" (:user result)))
      ;; %40 decodes to @
      (is (= "p@ss" (:password result))))))

;; =============================================================================
;; Pool configuration tests
;; =============================================================================

(deftest build-pool-config-test
  (testing "builds config from pg2/config"
    (let [config {:pg2/config {:host "localhost"
                               :port 5432
                               :user "testuser"
                               :password "testpass"
                               :database "testdb"}}
          result (pg2/build-pool-config config)]
      (is (= "localhost" (:host result)))
      (is (= 5432 (:port result)))
      (is (= "testuser" (:user result)))
      (is (= "testpass" (:password result)))
      (is (= "testdb" (:database result)))
      ;; Check defaults are applied
      (is (= 2 (:pool-min-size result)))
      (is (= 10 (:pool-max-size result)))))

  (testing "builds config from jdbc-url"
    (let [config {:pg2/jdbc-url "jdbc:postgresql://dbhost:5433/proddb?user=prod&password=secret"}
          result (pg2/build-pool-config config)]
      (is (= "dbhost" (:host result)))
      (is (= 5433 (:port result)))
      (is (= "proddb" (:database result)))
      (is (= "prod" (:user result)))
      (is (= "secret" (:password result)))))

  (testing "merges pool options"
    (let [config {:pg2/config {:host "localhost"
                               :port 5432
                               :database "testdb"}
                  :pg2/pool {:pool-min-size 5
                             :pool-max-size 20}}
          result (pg2/build-pool-config config)]
      (is (= 5 (:pool-min-size result)))
      (is (= 20 (:pool-max-size result)))))

  (testing "pool options override defaults"
    (let [config {:pg2/config {:host "localhost" :port 5432 :database "db"}
                  :pg2/pool {:pool-min-size 1}}
          result (pg2/build-pool-config config)]
      (is (= 1 (:pool-min-size result)))
      ;; pool-max-size should still get default
      (is (= 10 (:pool-max-size result)))))

  (testing "prefers jdbc-url over pg2/config when both present"
    ;; Note: build-pool-config checks jdbc-url first
    (let [config {:pg2/config {:host "fallback" :port 5432 :database "fallbackdb"}
                  :pg2/jdbc-url "jdbc:postgresql://primary:5433/maindb?user=x&password=y"}
          result (pg2/build-pool-config config)]
      (is (= "primary" (:host result)))
      (is (= "maindb" (:database result))))))

;; =============================================================================
;; Constraint handling tests
;; =============================================================================

(deftest add-referential-column-statement-test
  (testing "generates correct ALTER TABLE statement"
    (let [result (pg2/add-referential-column-statement
                  "accounts" "address_id" "UUID" "addresses" "id")]
      (is (= "ALTER TABLE accounts ADD COLUMN IF NOT EXISTS address_id UUID REFERENCES addresses(id) DEFERRABLE INITIALLY DEFERRED;\n"
             result))))

  (testing "handles different types"
    (let [result (pg2/add-referential-column-statement
                  "orders" "customer_id" "BIGINT" "customers" "id")]
      (is (= "ALTER TABLE orders ADD COLUMN IF NOT EXISTS customer_id BIGINT REFERENCES customers(id) DEFERRABLE INITIALLY DEFERRED;\n"
             result)))))
