(ns com.fulcrologic.rad.database-adapters.sql.schema-test
  "Tests for schema.clj - table/column name derivation from RAD attributes.
   Also tests migration.clj sql-type function for max-length option."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.schema :as schema]
   [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
   [taoensso.encore :as enc]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def key->attribute (enc/keys-by ::attr/qualified-key attrs/all-attributes))

;; =============================================================================
;; Unit Tests - column-name
;; =============================================================================

(deftest column-name-derives-from-qualified-key
  (testing "column-name derives snake_case from qualified-key name"
    (is (= "name" (schema/column-name attrs/account-name)))
    (is (= "street" (schema/column-name attrs/addr-street)))
    (is (= "city" (schema/column-name attrs/addr-city)))))

(deftest column-name-respects-explicit-column-name
  (testing "column-name uses explicit ::rad.sql/column-name when provided"
    ;; account-active? has explicit column-name "active"
    (is (= "active" (schema/column-name attrs/account-active?)))))

(deftest column-name-1-arity-throws-for-to-many-ref
  (testing "1-arity column-name throws for to-many ref attributes"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Cannot calculate column name for to-many ref without k->attr"
         (schema/column-name attrs/account-addresses)))

    ;; Verify the exception data contains the attribute key
    (try
      (schema/column-name attrs/account-addresses)
      (catch clojure.lang.ExceptionInfo e
        (is (= :account/addresses (:attr (ex-data e))))))))

(deftest column-name-2-arity-handles-to-many-ref
  (testing "2-arity column-name computes composite name for to-many refs"
    (let [col-name (schema/column-name key->attribute attrs/account-addresses)]
      ;; Should be something like "addresses_addresses_accounts_id"
      (is (string? col-name))
      (is (pos? (count col-name))))))

(deftest column-name-2-arity-delegates-for-non-ref
  (testing "2-arity column-name delegates to 1-arity for non-ref attributes"
    (is (= "name" (schema/column-name key->attribute attrs/account-name)))
    (is (= "active" (schema/column-name key->attribute attrs/account-active?)))))

;; =============================================================================
;; Unit Tests - table-name
;; =============================================================================

(deftest table-name-from-identity-attr
  (testing "table-name derives from identity attribute"
    (is (= "accounts" (schema/table-name attrs/account-id)))
    (is (= "addresses" (schema/table-name attrs/addr-id)))
    (is (= "users" (schema/table-name attrs/user-id)))))

(deftest table-name-throws-for-non-identity
  (testing "1-arity table-name throws for non-identity attributes"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"You must use an id-attribute with table-name"
         (schema/table-name attrs/account-name)))))

(deftest table-name-2-arity-derives-from-identities
  (testing "2-arity table-name derives table from attribute's identities"
    (is (= "accounts" (schema/table-name key->attribute attrs/account-name)))
    (is (= "addresses" (schema/table-name key->attribute attrs/addr-street)))))

;; =============================================================================
;; Unit Tests - sequence-name
;; =============================================================================

(deftest sequence-name-derives-correctly
  (testing "sequence-name combines table and column names"
    (is (= "accounts_id_seq" (schema/sequence-name attrs/account-id)))
    (is (= "addresses_id_seq" (schema/sequence-name attrs/addr-id)))))

;; =============================================================================
;; Unit Tests - tables-and-columns
;; =============================================================================

(deftest tables-and-columns-for-identity
  (testing "tables-and-columns returns single entry for identity attr"
    (is (= [["accounts" "id"]] (schema/tables-and-columns key->attribute attrs/account-id)))))

(deftest tables-and-columns-for-scalar
  (testing "tables-and-columns returns entry based on identities"
    (is (= [["accounts" "name"]] (schema/tables-and-columns key->attribute attrs/account-name)))))

;; =============================================================================
;; Invariants
;; =============================================================================

(def invariants
  {:column-name-returns-string
   {:id :column-name-returns-string
    :requirement "column-name always returns a non-empty string for valid attributes"
    :check (fn [{:keys [attr key->attribute use-2-arity?]}]
             (let [result (if use-2-arity?
                            (schema/column-name key->attribute attr)
                            (schema/column-name attr))]
               {:valid (and (string? result) (pos? (count result)))
                :result result}))}

   :table-name-returns-string
   {:id :table-name-returns-string
    :requirement "table-name always returns a non-empty string for identity attributes"
    :check (fn [{:keys [attr]}]
             (let [result (schema/table-name attr)]
               {:valid (and (string? result) (pos? (count result)))
                :result result}))}

   :column-name-is-snake-case
   {:id :column-name-is-snake-case
    :requirement "column-name returns snake_case (no hyphens, lowercase)"
    :check (fn [{:keys [result]}]
             {:valid (and (not (re-find #"-" result))
                          (= result (str/lower-case result)))
              :result result})}

   :to-many-ref-requires-k->attr
   {:id :to-many-ref-requires-k->attr
    :requirement "1-arity column-name throws for to-many ref attributes"
    :check (fn [{:keys [attr]}]
             (try
               (schema/column-name attr)
               {:valid false :reason "Should have thrown"}
               (catch clojure.lang.ExceptionInfo e
                 {:valid (= :attr (first (keys (ex-data e))))
                  :exception-data (ex-data e)})))}})

(defn check-invariant [invariant-key context]
  (let [invariant (get invariants invariant-key)
        result ((:check invariant) context)]
    (assoc result :invariant invariant-key)))

;; =============================================================================
;; Invariant Tests
;; =============================================================================

(deftest invariant-column-name-returns-string
  (testing "Invariant: column-name returns non-empty string"
    (doseq [attr [attrs/account-name attrs/account-email attrs/addr-street attrs/addr-city]]
      (let [result (check-invariant :column-name-returns-string
                                    {:attr attr :key->attribute key->attribute :use-2-arity? false})]
        (is (:valid result) (str "Failed for " (::attr/qualified-key attr)))))))

(deftest invariant-column-name-is-snake-case
  (testing "Invariant: column-name returns snake_case"
    (doseq [attr [attrs/account-name attrs/account-email attrs/addr-street]]
      (let [col-name (schema/column-name attr)
            result (check-invariant :column-name-is-snake-case {:result col-name})]
        (is (:valid result) (str "Failed for " (::attr/qualified-key attr) ": " col-name))))))

(deftest invariant-to-many-ref-requires-k->attr
  (testing "Invariant: to-many ref requires 2-arity"
    (let [result (check-invariant :to-many-ref-requires-k->attr
                                  {:attr attrs/account-addresses})]
      (is (:valid result)))))

;; =============================================================================
;; Generators
;; =============================================================================

(def gen-kebab-keyword-name
  "Generator for kebab-case keyword names (non-empty, valid identifier)"
  (gen/fmap
   (fn [parts]
     (clojure.string/join "-" parts))
   (gen/vector (gen/such-that #(pos? (count %))
                              (gen/fmap clojure.string/lower-case gen/string-alpha-numeric))
               1 3)))

(def gen-non-empty-kebab-name
  "Generator that ensures at least one non-empty part"
  (gen/such-that #(and (pos? (count %))
                       (re-matches #"[a-z][a-z0-9-]*" %))
                 gen-kebab-keyword-name))

(def gen-qualified-keyword
  "Generator for qualified keywords like :ns/name"
  (gen/fmap
   (fn [[ns-name kw-name]]
     (keyword ns-name kw-name))
   (gen/tuple gen-non-empty-kebab-name gen-non-empty-kebab-name)))

(def gen-snake-case-name
  "Generator for valid snake_case column names"
  (gen/such-that #(and (pos? (count %))
                       (re-matches #"[a-z][a-z0-9_]*" %))
                 (gen/fmap #(clojure.string/replace % "-" "_") gen-non-empty-kebab-name)))

(def gen-scalar-attr
  "Generator for scalar (non-ref) attributes"
  (gen/let [qk gen-qualified-keyword
            type (gen/elements [:string :boolean :int :uuid])
            has-explicit-col? gen/boolean
            explicit-col gen-snake-case-name]
    (cond-> {::attr/qualified-key qk
             ::attr/type type
             ::attr/cardinality :one}
      has-explicit-col? (assoc ::rad.sql/column-name explicit-col))))

(def gen-to-many-ref-attr
  "Generator for to-many ref attributes"
  (gen/let [qk gen-qualified-keyword]
    {::attr/qualified-key qk
     ::attr/type :ref
     ::attr/cardinality :many
     ::attr/target :some/id
     ::attr/identities #{:some/id}}))

;; =============================================================================
;; Generative Tests
;; =============================================================================

(defspec column-name-always-returns-string 50
  (prop/for-all [attr gen-scalar-attr]
                (let [result (schema/column-name attr)]
                  (and (string? result)
                       (pos? (count result))))))

(defspec column-name-never-contains-hyphens 50
  (prop/for-all [attr gen-scalar-attr]
                (let [result (schema/column-name attr)]
                  (not (re-find #"-" result)))))

(defspec column-name-is-lowercase 50
  (prop/for-all [attr gen-scalar-attr]
                (let [result (schema/column-name attr)]
                  (= result (clojure.string/lower-case result)))))

(defspec column-name-1-arity-throws-for-to-many-ref 30
  (prop/for-all [attr gen-to-many-ref-attr]
                (try
                  (schema/column-name attr)
                  false ;; Should have thrown
                  (catch clojure.lang.ExceptionInfo e
                    (and (= "Cannot calculate column name for to-many ref without k->attr."
                            (.getMessage e))
                         (contains? (ex-data e) :attr))))))

(defspec explicit-column-name-used-when-provided 30
  (prop/for-all [qk gen-qualified-keyword
                 explicit-col gen-snake-case-name]
                (let [attr {::attr/qualified-key qk
                            ::attr/type :string
                            ::attr/cardinality :one
                            ::rad.sql/column-name explicit-col}
                      result (schema/column-name attr)]
                  (= explicit-col result))))

;; =============================================================================
;; Unit Tests - sql-type and max-length
;; =============================================================================

(deftest sql-type-respects-max-length
  (testing "sql-type uses max-length for :string type"
    (let [attr-100 {::attr/qualified-key :test/name
                    ::attr/type :string
                    ::rad.sql/max-length 100}
          attr-500 {::attr/qualified-key :test/desc
                    ::attr/type :string
                    ::rad.sql/max-length 500}
          attr-no-max {::attr/qualified-key :test/other
                       ::attr/type :string}]
      (is (= "VARCHAR(100)" (mig/sql-type attr-100)))
      (is (= "VARCHAR(500)" (mig/sql-type attr-500)))
      (is (= "VARCHAR(200)" (mig/sql-type attr-no-max))
          "Without max-length, should default to VARCHAR(200)")))

  (testing "sql-type uses max-length for :password type"
    (let [attr-256 {::attr/qualified-key :test/secret
                    ::attr/type :password
                    ::rad.sql/max-length 256}
          attr-no-max {::attr/qualified-key :test/pass
                       ::attr/type :password}]
      (is (= "VARCHAR(256)" (mig/sql-type attr-256)))
      (is (= "VARCHAR(200)" (mig/sql-type attr-no-max)))))

  (testing "sql-type uses max-length for :keyword type"
    (let [attr-50 {::attr/qualified-key :test/status
                   ::attr/type :keyword
                   ::rad.sql/max-length 50}
          attr-no-max {::attr/qualified-key :test/category
                       ::attr/type :keyword}]
      (is (= "VARCHAR(50)" (mig/sql-type attr-50)))
      (is (= "VARCHAR(200)" (mig/sql-type attr-no-max)))))

  (testing "sql-type uses max-length for :symbol type"
    (let [attr-75 {::attr/qualified-key :test/sym
                   ::attr/type :symbol
                   ::rad.sql/max-length 75}
          attr-no-max {::attr/qualified-key :test/sym2
                       ::attr/type :symbol}]
      (is (= "VARCHAR(75)" (mig/sql-type attr-75)))
      (is (= "VARCHAR(200)" (mig/sql-type attr-no-max)))))

  (testing "sql-type ignores max-length for non-string types"
    (let [int-attr {::attr/qualified-key :test/count
                    ::attr/type :int
                    ::rad.sql/max-length 100}
          uuid-attr {::attr/qualified-key :test/id
                     ::attr/type :uuid
                     ::rad.sql/max-length 100}
          bool-attr {::attr/qualified-key :test/active
                     ::attr/type :boolean
                     ::rad.sql/max-length 100}
          decimal-attr {::attr/qualified-key :test/price
                        ::attr/type :decimal
                        ::rad.sql/max-length 100}
          instant-attr {::attr/qualified-key :test/created
                        ::attr/type :instant
                        ::rad.sql/max-length 100}]
      (is (= "INTEGER" (mig/sql-type int-attr)))
      (is (= "UUID" (mig/sql-type uuid-attr)))
      (is (= "BOOLEAN" (mig/sql-type bool-attr)))
      (is (= "decimal(20,2)" (mig/sql-type decimal-attr)))
      (is (= "TIMESTAMP WITH TIME ZONE" (mig/sql-type instant-attr))))))

(deftest sql-type-enum-uses-fixed-length
  (testing ":enum type uses fixed VARCHAR(200) regardless of max-length"
    ;; Note: enum intentionally doesn't use max-length per migration.clj comment
    ;; "There is no standard SQL enum, and many ppl think they are a bad idea"
    (let [attr {::attr/qualified-key :test/state
                ::attr/type :enum}]
      (is (= "VARCHAR(200)" (mig/sql-type attr))))))

(deftest sql-type-long-type
  (testing ":long type maps to BIGINT"
    (let [attr {::attr/qualified-key :test/big-number
                ::attr/type :long}]
      (is (= "BIGINT" (mig/sql-type attr))))))

;; =============================================================================
;; Generative Tests - max-length
;; =============================================================================

(def gen-max-length
  "Generator for valid max-length values (positive integers)"
  (gen/such-that pos? (gen/choose 1 10000)))

(defspec max-length-string-types-use-custom-length 50
  (prop/for-all [max-len gen-max-length
                 type (gen/elements [:string :password :keyword :symbol])]
                (let [attr {::attr/qualified-key :test/attr
                            ::attr/type type
                            ::rad.sql/max-length max-len}
                      result (mig/sql-type attr)]
                  (= (str "VARCHAR(" max-len ")") result))))

(defspec max-length-ignored-for-non-string-types 30
  (prop/for-all [max-len gen-max-length
                 type (gen/elements [:int :long :boolean :uuid :decimal :instant])]
                (let [attr-with-max {::attr/qualified-key :test/attr
                                     ::attr/type type
                                     ::rad.sql/max-length max-len}
                      attr-without-max {::attr/qualified-key :test/attr
                                        ::attr/type type}]
                  ;; max-length should have no effect on non-string types
                  (= (mig/sql-type attr-with-max)
                     (mig/sql-type attr-without-max)))))
