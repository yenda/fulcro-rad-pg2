(ns com.fulcrologic.rad.database-adapters.sql.pg2-test
  "Tests for pg2 type transformation functions."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2])
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
    (let [now (Instant/now)
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
