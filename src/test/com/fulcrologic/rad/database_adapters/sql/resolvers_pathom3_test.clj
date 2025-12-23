(ns com.fulcrologic.rad.database-adapters.sql.resolvers-pathom3-test
  "Unit tests for resolvers-pathom3 helper functions."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.database-adapters.sql.resolvers-pathom3 :as sut]))

;; =============================================================================
;; reorder-maps-by-id tests
;; =============================================================================

(deftest reorder-maps-by-id-test
  (testing "reorders target-vec to match order-vec based on id-key"
    (let [order-vec [{:l/id 3} {:l/id 10} {:l/id "w"} {:l/id 1}]
          target-vec [{:l/id 1 :value "a"} {:l/id "w" :value "b"} {:l/id 3 :value "c"}]]
      (is (= [{:l/id 3 :value "c"} {:l/id 10} {:l/id "w" :value "b"} {:l/id 1 :value "a"}]
             (sut/reorder-maps-by-id :l/id order-vec target-vec)))))

  (testing "preserves order-vec entries without matches in target-vec"
    (let [order-vec [{:id 1} {:id 2} {:id 3}]
          target-vec [{:id 3 :name "C"} {:id 1 :name "A"}]]
      (is (= [{:id 1 :name "A"} {:id 2} {:id 3 :name "C"}]
             (sut/reorder-maps-by-id :id order-vec target-vec)))))

  (testing "handles empty order-vec"
    (is (= [] (sut/reorder-maps-by-id :id [] [{:id 1}]))))

  (testing "handles empty target-vec"
    (is (= [{:id 1} {:id 2}]
           (sut/reorder-maps-by-id :id [{:id 1} {:id 2}] []))))

  (testing "handles both empty"
    (is (= [] (sut/reorder-maps-by-id :id [] []))))

  (testing "works with different id-keys"
    (let [order-vec [{:foo/bar "x"} {:foo/bar "y"}]
          target-vec [{:foo/bar "y" :data 2} {:foo/bar "x" :data 1}]]
      (is (= [{:foo/bar "x" :data 1} {:foo/bar "y" :data 2}]
             (sut/reorder-maps-by-id :foo/bar order-vec target-vec)))))

  (testing "works with UUID ids"
    (let [id1 (java.util.UUID/randomUUID)
          id2 (java.util.UUID/randomUUID)
          order-vec [{:id id2} {:id id1}]
          target-vec [{:id id1 :name "first"} {:id id2 :name "second"}]]
      (is (= [{:id id2 :name "second"} {:id id1 :name "first"}]
             (sut/reorder-maps-by-id :id order-vec target-vec))))))

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
