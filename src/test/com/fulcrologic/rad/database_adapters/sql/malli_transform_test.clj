(ns com.fulcrologic.rad.database-adapters.sql.malli-transform-test
  "Tests for SQL type transformation functions."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.malli-transform :as mt]
   [malli.core :as m])
  (:import
   [java.sql Timestamp]
   [java.time Instant]))

;; =============================================================================
;; Helper function tests
;; =============================================================================

(deftest keyword->sql-string-test
  (testing "converts keywords to strings with leading colon"
    (is (= ":foo" (mt/keyword->sql-string :foo)))
    (is (= ":foo/bar" (mt/keyword->sql-string :foo/bar)))
    (is (= ":state/AZ" (mt/keyword->sql-string :state/AZ))))
  (testing "passes through non-keywords"
    (is (= "hello" (mt/keyword->sql-string "hello")))
    (is (= 42 (mt/keyword->sql-string 42)))
    (is (nil? (mt/keyword->sql-string nil)))))

(deftest sql-string->keyword-test
  (testing "converts strings to keywords, handling leading colon"
    (is (= :foo (mt/sql-string->keyword ":foo")))
    (is (= :foo/bar (mt/sql-string->keyword ":foo/bar")))
    (is (= :state/AZ (mt/sql-string->keyword ":state/AZ"))))
  (testing "handles strings without leading colon"
    (is (= :foo (mt/sql-string->keyword "foo")))
    (is (= :foo/bar (mt/sql-string->keyword "foo/bar"))))
  (testing "passes through non-strings"
    (is (= :already (mt/sql-string->keyword :already)))
    (is (= 42 (mt/sql-string->keyword 42)))))

(deftest symbol->sql-string-test
  (testing "converts symbols to strings"
    (is (= "foo" (mt/symbol->sql-string 'foo)))
    (is (= "my.ns/fn" (mt/symbol->sql-string 'my.ns/fn))))
  (testing "passes through non-symbols"
    (is (= "hello" (mt/symbol->sql-string "hello")))
    (is (= :key (mt/symbol->sql-string :key)))))

(deftest sql-string->symbol-test
  (testing "converts strings to symbols"
    (is (= 'foo (mt/sql-string->symbol "foo")))
    (is (= 'my.ns/fn (mt/sql-string->symbol "my.ns/fn"))))
  (testing "passes through non-strings"
    (is (= 'already (mt/sql-string->symbol 'already)))))

(deftest instant->timestamp-test
  (let [now (Instant/now)]
    (testing "converts Instant to Timestamp"
      (let [result (mt/instant->timestamp now)]
        (is (instance? Timestamp result))
        (is (= (.toEpochMilli now) (.getTime ^Timestamp result)))))
    (testing "passes through Timestamps"
      (let [ts (Timestamp/from now)]
        (is (identical? ts (mt/instant->timestamp ts)))))
    (testing "passes through other values"
      (is (= "hello" (mt/instant->timestamp "hello"))))))

(deftest timestamp->instant-test
  (let [now (Instant/now)
        ts (Timestamp/from now)]
    (testing "converts Timestamp to Instant"
      (let [result (mt/timestamp->instant ts)]
        (is (instance? Instant result))
        (is (= (.getTime ts) (.toEpochMilli ^Instant result)))))
    (testing "passes through Instants"
      (is (identical? now (mt/timestamp->instant now))))
    (testing "passes through other values"
      (is (= "hello" (mt/timestamp->instant "hello"))))))

;; =============================================================================
;; Public API tests
;; =============================================================================

(deftest encode-for-sql-test
  (testing "encodes keywords/enums to strings"
    (is (= ":status/active" (mt/encode-for-sql :enum :status/active)))
    (is (= ":foo" (mt/encode-for-sql :keyword :foo))))
  (testing "encodes symbols to strings"
    (is (= "my.ns/fn" (mt/encode-for-sql :symbol 'my.ns/fn))))
  (testing "encodes instants to timestamps"
    (let [now (Instant/now)]
      (is (instance? Timestamp (mt/encode-for-sql :instant now)))))
  (testing "passes through other types"
    (is (= "hello" (mt/encode-for-sql :string "hello")))
    (is (= 42 (mt/encode-for-sql :int 42)))
    (is (= true (mt/encode-for-sql :boolean true))))
  (testing "returns nil for nil input"
    (is (nil? (mt/encode-for-sql :keyword nil)))
    (is (nil? (mt/encode-for-sql :instant nil)))))

(deftest decode-from-sql-test
  (testing "decodes strings to keywords/enums"
    (is (= :status/active (mt/decode-from-sql :enum ":status/active")))
    (is (= :foo (mt/decode-from-sql :keyword ":foo")))
    (is (= :foo (mt/decode-from-sql :keyword "foo"))))
  (testing "decodes strings to symbols"
    (is (= 'my.ns/fn (mt/decode-from-sql :symbol "my.ns/fn"))))
  (testing "decodes timestamps to instants"
    (let [ts (Timestamp/from (Instant/now))]
      (is (instance? Instant (mt/decode-from-sql :instant ts)))))
  (testing "passes through other types"
    (is (= "hello" (mt/decode-from-sql :string "hello")))
    (is (= 42 (mt/decode-from-sql :int 42)))
    (is (= true (mt/decode-from-sql :boolean true))))
  (testing "returns nil for nil input"
    (is (nil? (mt/decode-from-sql :keyword nil)))
    (is (nil? (mt/decode-from-sql :instant nil)))))

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
          (let [encoded (mt/encode-for-sql rad-type value)
                decoded (mt/decode-from-sql rad-type encoded)]
            (is (= value decoded))))))))

;; =============================================================================
;; Custom registration tests
;; =============================================================================

(deftest register-encoder-test
  (let [original-encoders @mt/encoders]
    (try
      (testing "registers custom encoder"
        (mt/register-encoder! :test-type (fn [x] (str "TEST:" x)))
        (is (= "TEST:hello" (mt/encode-for-sql :test-type "hello"))))
      (finally
        (reset! mt/encoders original-encoders)))))

(deftest register-decoder-test
  (let [original-decoders @mt/decoders]
    (try
      (testing "registers custom decoder"
        (mt/register-decoder! :test-type (fn [x] (keyword (subs x 5))))
        (is (= :hello (mt/decode-from-sql :test-type "TEST:hello"))))
      (finally
        (reset! mt/decoders original-decoders)))))

;; =============================================================================
;; Malli transformer tests
;; =============================================================================

(deftest sql-transformer-test
  (testing "encodes map with keywords"
    (let [schema [:map [:status :keyword] [:name :string]]
          data {:status :active :name "Alice"}
          encoded (m/encode schema data mt/sql-transformer)]
      (is (string? (:status encoded)))
      (is (= ":active" (:status encoded)))
      (is (= "Alice" (:name encoded)))))
  (testing "decodes map with keywords"
    (let [schema [:map [:status :keyword] [:name :string]]
          sql-data {:status ":active" :name "Alice"}
          decoded (m/decode schema sql-data mt/sql-transformer)]
      (is (keyword? (:status decoded)))
      (is (= :active (:status decoded)))
      (is (= "Alice" (:name decoded))))))

(deftest cached-encoder-test
  (testing "returns consistent function for same schema"
    (let [schema [:map [:x :keyword]]
          encoder1 (mt/cached-encoder schema)
          encoder2 (mt/cached-encoder schema)]
      (is (identical? encoder1 encoder2))))
  (testing "encodes correctly"
    (let [schema [:map [:status :keyword]]
          encoder (mt/cached-encoder schema)
          result (encoder {:status :active})]
      (is (= ":active" (:status result))))))

(deftest cached-decoder-test
  (testing "returns consistent function for same schema"
    (let [schema [:map [:x :keyword]]
          decoder1 (mt/cached-decoder schema)
          decoder2 (mt/cached-decoder schema)]
      (is (identical? decoder1 decoder2))))
  (testing "decodes correctly"
    (let [schema [:map [:status :keyword]]
          decoder (mt/cached-decoder schema)
          result (decoder {:status ":active"})]
      (is (= :active (:status result))))))

;; =============================================================================
;; RAD Attribute -> Malli Schema Generation Tests
;; =============================================================================

(deftest rad-type->malli-schema-test
  (testing "keyword type gets decode/sql transformer"
    (let [schema (mt/rad-type->malli-schema :keyword)]
      (is (vector? schema))
      (is (= :any (first schema)))
      (is (fn? (get-in schema [1 :decode/sql])))))

  (testing "enum type gets decode/sql transformer"
    (let [schema (mt/rad-type->malli-schema :enum)]
      (is (vector? schema))
      (is (= :any (first schema)))
      (is (fn? (get-in schema [1 :decode/sql])))))

  (testing "symbol type gets decode/sql transformer"
    (let [schema (mt/rad-type->malli-schema :symbol)]
      (is (vector? schema))
      (is (fn? (get-in schema [1 :decode/sql])))))

  (testing "instant type gets decode/sql transformer"
    (let [schema (mt/rad-type->malli-schema :instant)]
      (is (vector? schema))
      (is (fn? (get-in schema [1 :decode/sql])))))

  (testing "other types return :any without transformer"
    (is (= :any (mt/rad-type->malli-schema :string)))
    (is (= :any (mt/rad-type->malli-schema :int)))
    (is (= :any (mt/rad-type->malli-schema :uuid)))
    (is (= :any (mt/rad-type->malli-schema :boolean)))))

(deftest attribute->column-key-test
  (testing "uses column-name when present"
    (let [attr {::attr/qualified-key :user/status
                ::rad.sql/column-name "user_status"}]
      (is (= :user_status
             (mt/attribute->column-key attr
                                       ::attr/qualified-key
                                       ::rad.sql/column-name)))))

  (testing "derives from qualified-key when column-name absent"
    (let [attr {::attr/qualified-key :user/status}]
      (is (= :status
             (mt/attribute->column-key attr
                                       ::attr/qualified-key
                                       ::rad.sql/column-name))))))

(deftest attributes->malli-schema-test
  (testing "generates map schema from attributes"
    (let [attrs [{::attr/qualified-key :user/id
                  ::attr/type :uuid}
                 {::attr/qualified-key :user/status
                  ::attr/type :keyword}
                 {::attr/qualified-key :user/name
                  ::attr/type :string}]
          schema (mt/attributes->malli-schema attrs
                                              ::attr/qualified-key
                                              ::rad.sql/column-name
                                              ::attr/type)]
      (is (= :map (first schema)))
      (is (= {:closed false} (second schema)))
      ;; Should have 3 entries
      (is (= 3 (count (drop 2 schema))))))

  (testing "uses custom column names"
    (let [attrs [{::attr/qualified-key :user/status
                  ::rad.sql/column-name "user_status"
                  ::attr/type :keyword}]
          schema (mt/attributes->malli-schema attrs
                                              ::attr/qualified-key
                                              ::rad.sql/column-name
                                              ::attr/type)]
      ;; First entry should be [:user_status ...]
      (is (= :user_status (first (nth schema 2)))))))

(deftest compile-row-decoder-test
  (testing "decodes keyword columns"
    (let [attrs [{::attr/qualified-key :user/id
                  ::attr/type :uuid}
                 {::attr/qualified-key :user/status
                  ::attr/type :keyword}]
          decoder (mt/compile-row-decoder attrs
                                          ::attr/qualified-key
                                          ::rad.sql/column-name
                                          ::attr/type)
          sql-row {:id #uuid "550e8400-e29b-41d4-a716-446655440000"
                   :status ":active"}
          result (decoder sql-row)]
      (is (= :active (:status result)))
      (is (= #uuid "550e8400-e29b-41d4-a716-446655440000" (:id result)))))

  (testing "decodes instant columns"
    (let [now (Instant/now)
          ts (Timestamp/from now)
          attrs [{::attr/qualified-key :user/created-at
                  ::attr/type :instant}]
          decoder (mt/compile-row-decoder attrs
                                          ::attr/qualified-key
                                          ::rad.sql/column-name
                                          ::attr/type)
          sql-row {:created-at ts}
          result (decoder sql-row)]
      (is (instance? Instant (:created-at result)))
      (is (= (.toEpochMilli now) (.toEpochMilli ^Instant (:created-at result))))))

  (testing "decodes symbol columns"
    (let [attrs [{::attr/qualified-key :event/handler
                  ::attr/type :symbol}]
          decoder (mt/compile-row-decoder attrs
                                          ::attr/qualified-key
                                          ::rad.sql/column-name
                                          ::attr/type)
          sql-row {:handler "my.ns/handler-fn"}
          result (decoder sql-row)]
      (is (symbol? (:handler result)))
      (is (= 'my.ns/handler-fn (:handler result)))))

  (testing "passes through non-transformable types"
    (let [attrs [{::attr/qualified-key :user/name
                  ::attr/type :string}
                 {::attr/qualified-key :user/age
                  ::attr/type :int}
                 {::attr/qualified-key :user/active
                  ::attr/type :boolean}]
          decoder (mt/compile-row-decoder attrs
                                          ::attr/qualified-key
                                          ::rad.sql/column-name
                                          ::attr/type)
          sql-row {:name "Alice" :age 30 :active true}
          result (decoder sql-row)]
      (is (= "Alice" (:name result)))
      (is (= 30 (:age result)))
      (is (= true (:active result)))))

  (testing "handles nil values"
    (let [attrs [{::attr/qualified-key :user/status
                  ::attr/type :keyword}]
          decoder (mt/compile-row-decoder attrs
                                          ::attr/qualified-key
                                          ::rad.sql/column-name
                                          ::attr/type)
          sql-row {:status nil}
          result (decoder sql-row)]
      (is (nil? (:status result)))))

  (testing "preserves extra columns not in schema"
    (let [attrs [{::attr/qualified-key :user/status
                  ::attr/type :keyword}]
          decoder (mt/compile-row-decoder attrs
                                          ::attr/qualified-key
                                          ::rad.sql/column-name
                                          ::attr/type)
          sql-row {:status ":active" :extra "value"}
          result (decoder sql-row)]
      (is (= :active (:status result)))
      ;; Extra columns should be preserved (open map)
      (is (= "value" (:extra result))))))

(deftest compile-row-decoder-mixed-types-test
  (testing "decodes row with multiple transformable types"
    (let [now (Instant/now)
          ts (Timestamp/from now)
          attrs [{::attr/qualified-key :event/id
                  ::attr/type :uuid}
                 {::attr/qualified-key :event/type
                  ::attr/type :keyword}
                 {::attr/qualified-key :event/handler
                  ::attr/type :symbol}
                 {::attr/qualified-key :event/created-at
                  ::attr/type :instant}
                 {::attr/qualified-key :event/payload
                  ::attr/type :string}]
          decoder (mt/compile-row-decoder attrs
                                          ::attr/qualified-key
                                          ::rad.sql/column-name
                                          ::attr/type)
          sql-row {:id #uuid "550e8400-e29b-41d4-a716-446655440000"
                   :type ":user/created"
                   :handler "my.app/process-event"
                   :created-at ts
                   :payload "{\"foo\": \"bar\"}"}
          result (decoder sql-row)]
      (is (= #uuid "550e8400-e29b-41d4-a716-446655440000" (:id result)))
      (is (= :user/created (:type result)))
      (is (= 'my.app/process-event (:handler result)))
      (is (instance? Instant (:created-at result)))
      (is (= "{\"foo\": \"bar\"}" (:payload result))))))
