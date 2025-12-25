(ns com.fulcrologic.rad.database-adapters.pg2.read-test
  "Unit tests for read resolver helper functions."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.database-adapters.pg2.read :as sut]))

;; =============================================================================
;; get-column tests
;; =============================================================================

(deftest get-column-test
  (testing "returns ::rad.pg2/column-name when present"
    (is (= :my_column
           (sut/get-column {:com.fulcrologic.rad.database-adapters.pg2/column-name "my_column"}))))

  (testing "derives snake_case from qualified-key name"
    (is (= :name
           (sut/get-column {:com.fulcrologic.rad.attributes/qualified-key :user/name}))))

  (testing "converts kebab-case to snake_case"
    (is (= :primary_address
           (sut/get-column {:com.fulcrologic.rad.attributes/qualified-key :account/primary-address}))))

  (testing "throws when no column can be derived"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Can't find column name"
                          (sut/get-column {})))))

;; =============================================================================
;; get-table tests
;; =============================================================================

(deftest get-table-test
  (testing "returns keyword from ::rad.pg2/table"
    (is (= :users
           (sut/get-table {:com.fulcrologic.rad.database-adapters.pg2/table "users"}))))

  (testing "returns nil when no table"
    (is (nil? (sut/get-table {})))))

;; =============================================================================
;; to-one-keyword tests
;; =============================================================================

(deftest to-one-keyword-test
  (testing "appends -id to qualified keyword"
    (is (= :user/address-id
           (sut/to-one-keyword :user/address))))

  (testing "works with various namespaces"
    (is (= :organization/headquarters-id
           (sut/to-one-keyword :organization/headquarters)))))

;; =============================================================================
;; get-pg-array-type tests
;; =============================================================================

(deftest get-pg-array-type-test
  (testing "returns correct array type for common ID types"
    (is (= "uuid[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :uuid})))
    (is (= "int4[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :int})))
    (is (= "int8[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :long})))
    (is (= "text[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :string}))))

  (testing "returns correct array type for less common types"
    (is (= "boolean[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :boolean})))
    (is (= "numeric[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :decimal})))
    (is (= "timestamptz[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :instant}))))

  (testing "returns text[] for keyword/enum types (stored as text)"
    (is (= "text[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :keyword})))
    (is (= "text[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :enum}))))

  (testing "defaults to text[] for unknown types"
    (is (= "text[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :unknown})))))

;; =============================================================================
;; build-select-by-ids-sql tests
;; =============================================================================

(deftest build-select-by-ids-sql-test
  (testing "builds basic SELECT query with single column"
    (is (= "SELECT id FROM users WHERE id = ANY($1::uuid[])"
           (sut/build-select-by-ids-sql [:id] :users :id "uuid[]"))))

  (testing "builds SELECT query with multiple columns"
    (is (= "SELECT id, name, email FROM users WHERE id = ANY($1::uuid[])"
           (sut/build-select-by-ids-sql [:id :name :email] :users :id "uuid[]"))))

  (testing "works with different table names"
    (is (= "SELECT id, title FROM documents WHERE id = ANY($1::uuid[])"
           (sut/build-select-by-ids-sql [:id :title] :documents :id "uuid[]"))))

  (testing "works with different array types"
    (is (= "SELECT id, value FROM items WHERE id = ANY($1::int4[])"
           (sut/build-select-by-ids-sql [:id :value] :items :id "int4[]")))
    (is (= "SELECT id FROM events WHERE id = ANY($1::int8[])"
           (sut/build-select-by-ids-sql [:id] :events :id "int8[]"))))

  (testing "handles snake_case column names"
    (is (= "SELECT id, created_at, updated_at FROM accounts WHERE id = ANY($1::uuid[])"
           (sut/build-select-by-ids-sql [:id :created_at :updated_at] :accounts :id "uuid[]")))))

;; =============================================================================
;; build-array-agg-sql tests
;; =============================================================================

(deftest build-array-agg-sql-test
  (testing "builds basic array_agg query without ordering"
    (is (= "SELECT account_id AS k, array_agg(id) AS v FROM addresses WHERE account_id = ANY($1::uuid[]) GROUP BY account_id"
           (sut/build-array-agg-sql :account_id :id :addresses "uuid[]" nil))))

  (testing "builds array_agg query with ordering"
    (is (= "SELECT account_id AS k, array_agg(id ORDER BY position) AS v FROM addresses WHERE account_id = ANY($1::uuid[]) GROUP BY account_id"
           (sut/build-array-agg-sql :account_id :id :addresses "uuid[]" :position))))

  (testing "works with different table and column names"
    (is (= "SELECT parent_id AS k, array_agg(id) AS v FROM categories WHERE parent_id = ANY($1::uuid[]) GROUP BY parent_id"
           (sut/build-array-agg-sql :parent_id :id :categories "uuid[]" nil))))

  (testing "works with different array types"
    (is (= "SELECT user_id AS k, array_agg(id ORDER BY created_at) AS v FROM posts WHERE user_id = ANY($1::int4[]) GROUP BY user_id"
           (sut/build-array-agg-sql :user_id :id :posts "int4[]" :created_at)))))
