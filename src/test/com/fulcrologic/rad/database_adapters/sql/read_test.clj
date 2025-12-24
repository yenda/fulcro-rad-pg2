(ns com.fulcrologic.rad.database-adapters.sql.read-test
  "Unit tests for read resolver helper functions."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.database-adapters.sql.read :as sut]))

;; =============================================================================
;; get-column tests
;; =============================================================================

(deftest get-column-test
  (testing "returns ::rad.sql/column-name when present"
    (is (= :my_column
           (sut/get-column {:com.fulcrologic.rad.database-adapters.sql/column-name "my_column"}))))

  (testing "returns :column when present and no ::rad.sql/column-name"
    (is (= :other_col
           (sut/get-column {:column "other_col"}))))

  (testing "derives from qualified-key name when no explicit column"
    (is (= :name
           (sut/get-column {:com.fulcrologic.rad.attributes/qualified-key :user/name}))))

  (testing "throws when no column can be derived"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Can't find column name"
                          (sut/get-column {})))))

;; =============================================================================
;; get-table tests
;; =============================================================================

(deftest get-table-test
  (testing "returns keyword from ::rad.sql/table"
    (is (= :users
           (sut/get-table {:com.fulcrologic.rad.database-adapters.sql/table "users"}))))

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
  (testing "returns correct array type for each RAD type"
    (is (= "uuid[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :uuid})))
    (is (= "int4[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :int})))
    (is (= "int8[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :long})))
    (is (= "text[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :string}))))

  (testing "defaults to text[] for unknown types"
    (is (= "text[]" (sut/get-pg-array-type {:com.fulcrologic.rad.attributes/type :unknown})))))
