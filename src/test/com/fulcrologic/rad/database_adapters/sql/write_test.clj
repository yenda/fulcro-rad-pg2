(ns com.fulcrologic.rad.database-adapters.sql.write-test
  "Unit tests for pure functions in write.clj.

   These tests verify the delta transformation logic without touching the database."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.write :as write]))

;; =============================================================================
;; Test Attribute Definitions (minimal, for unit testing)
;; =============================================================================

(def test-key->attribute
  "Minimal key->attribute map for testing process-ref-attributes."
  {;; Course/LanguageCourse pattern (child owns FK, parent has fk-attr)
   :course/language-course
   {::attr/qualified-key :course/language-course
    ::attr/type :ref
    ::attr/cardinality :one
    ::attr/target :language-course/id
    ::rad.sql/fk-attr :language-course/course
    ::rad.sql/delete-orphan? true}

   :language-course/course
   {::attr/qualified-key :language-course/course
    ::attr/type :ref
    ::attr/cardinality :one
    ::attr/target :course/id}

   ;; Organization/Domains pattern (to-many with delete-orphan)
   :organization/domains
   {::attr/qualified-key :organization/domains
    ::attr/type :ref
    ::attr/cardinality :many
    ::attr/target :domain/id
    ::rad.sql/fk-attr :domain/organization
    ::rad.sql/delete-orphan? true}

   :domain/organization
   {::attr/qualified-key :domain/organization
    ::attr/type :ref
    ::attr/cardinality :one
    ::attr/target :organization/id}

   ;; Simple fk-attr without delete-orphan (account/address)
   :account/address
   {::attr/qualified-key :account/address
    ::attr/type :ref
    ::attr/cardinality :one
    ::attr/target :address/id
    ::rad.sql/fk-attr :address/account}

   :address/account
   {::attr/qualified-key :address/account
    ::attr/type :ref
    ::attr/cardinality :one
    ::attr/target :account/id}

   ;; Direct FK (no ref) - scalar-like ref
   :employee/department
   {::attr/qualified-key :employee/department
    ::attr/type :ref
    ::attr/cardinality :one
    ::attr/target :department/id}})

;; =============================================================================
;; process-ref-attributes Tests
;; =============================================================================

(deftest process-ref-attributes-one-add-new-ref
  (testing "Adding a new one-to-one reference generates FK update on target"
    (let [delta {[:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000001"]
                 {:course/language-course {:before nil
                                           :after [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000001"]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Original delta entry preserved
      (is (contains? result [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000001"]))
      ;; New entry for target to receive FK update
      (is (= {:language-course/course {:after [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000001"]}}
             (get result [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000001"]))))))

(deftest process-ref-attributes-one-remove-ref-no-delete
  (testing "Removing reference without delete-referent? clears FK on target"
    (let [delta {[:account/id #uuid "a1a1a1a1-0000-0000-0000-000000000001"]
                 {:account/address {:before [:address/id #uuid "add1add1-0000-0000-0000-000000000001"]
                                    :after nil}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Target should have FK cleared (before set, after nil)
      (is (= {:address/account {:before [:account/id #uuid "a1a1a1a1-0000-0000-0000-000000000001"]
                                :after nil}}
             (get result [:address/id #uuid "add1add1-0000-0000-0000-000000000001"]))))))

(deftest process-ref-attributes-one-remove-ref-with-delete
  (testing "Removing reference with delete-referent? marks target for deletion"
    (let [delta {[:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000002"]
                 {:course/language-course {:before [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000002"]
                                           :after nil}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Target should be marked for deletion
      (is (= {:language-course/course {:before [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000002"]
                                       :after :delete}}
             (get result [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000002"]))))))

(deftest process-ref-attributes-one-change-ref-with-delete
  (testing "Changing reference (A→B) with delete-referent? updates B and deletes A"
    (let [delta {[:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000003"]
                 {:course/language-course {:before [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-0000000000aa"]
                                           :after [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-0000000000bb"]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; New target should receive FK
      (is (= {:language-course/course {:after [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000003"]}}
             (get result [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-0000000000bb"])))
      ;; Old target should be deleted
      (is (= {:language-course/course {:before [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000003"]
                                       :after :delete}}
             (get result [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-0000000000aa"]))))))

(deftest process-ref-attributes-one-change-ref-without-delete
  (testing "Changing reference (A→B) without delete-referent? updates B and clears A"
    (let [delta {[:account/id #uuid "a1a1a1a1-0000-0000-0000-000000000002"]
                 {:account/address {:before [:address/id #uuid "add1add1-0000-0000-0000-0000000000aa"]
                                    :after [:address/id #uuid "add1add1-0000-0000-0000-0000000000bb"]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; New target should receive FK
      (is (= {:address/account {:after [:account/id #uuid "a1a1a1a1-0000-0000-0000-000000000002"]}}
             (get result [:address/id #uuid "add1add1-0000-0000-0000-0000000000bb"])))
      ;; Old target should have FK cleared (not deleted)
      (is (= {:address/account {:before [:account/id #uuid "a1a1a1a1-0000-0000-0000-000000000002"]
                                :after nil}}
             (get result [:address/id #uuid "add1add1-0000-0000-0000-0000000000aa"]))))))

(deftest process-ref-attributes-one-no-change
  (testing "Same before and after (no change) only updates target FK once"
    (let [delta {[:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000004"]
                 {:course/language-course {:before [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000004"]
                                           :after [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000004"]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Should update the FK (idempotent, but no deletion)
      (is (= {:language-course/course {:after [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000004"]}}
             (get result [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000004"])))
      ;; Should only have 2 entries (original + target update)
      (is (= 2 (count result))))))

(deftest process-ref-attributes-direct-fk-ignored
  (testing "Direct FK refs (no `ref` option) are not processed"
    (let [delta {[:employee/id #uuid "e1e1e1e1-0000-0000-0000-000000000001"]
                 {:employee/department {:before nil
                                        :after [:department/id #uuid "d1d1d1d1-0000-0000-0000-000000000001"]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Original delta unchanged, no additional entries
      (is (= delta result))
      (is (= 1 (count result))))))

;; =============================================================================
;; process-ref-attributes Tests - :many cardinality
;; =============================================================================

(deftest process-ref-attributes-many-add-items
  (testing "Adding items to to-many ref generates FK updates for each"
    (let [delta {[:organization/id #uuid "01010101-0000-0000-0000-000000000001"]
                 {:organization/domains {:before []
                                         :after [[:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000001"]
                                                 [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000002"]]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Each new domain should receive FK to organization
      (is (= {:domain/organization {:after [:organization/id #uuid "01010101-0000-0000-0000-000000000001"]}}
             (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000001"])))
      (is (= {:domain/organization {:after [:organization/id #uuid "01010101-0000-0000-0000-000000000001"]}}
             (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000002"]))))))

(deftest process-ref-attributes-many-remove-items-with-delete
  (testing "Removing items from to-many with delete-referent? marks them for deletion"
    (let [delta {[:organization/id #uuid "01010101-0000-0000-0000-000000000002"]
                 {:organization/domains {:before [[:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000003"]
                                                  [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000004"]]
                                         :after []}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Both domains should be marked for deletion
      (is (= {:domain/organization {:before [:organization/id #uuid "01010101-0000-0000-0000-000000000002"]
                                    :after :delete}}
             (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000003"])))
      (is (= {:domain/organization {:before [:organization/id #uuid "01010101-0000-0000-0000-000000000002"]
                                    :after :delete}}
             (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000004"]))))))

(deftest process-ref-attributes-many-mixed-add-remove
  (testing "Adding some and removing others in same operation"
    (let [delta {[:organization/id #uuid "01010101-0000-0000-0000-000000000003"]
                 {:organization/domains {:before [[:domain/id #uuid "d0d0d0d0-0000-0000-0000-0000000000aa"]]
                                         :after [[:domain/id #uuid "d0d0d0d0-0000-0000-0000-0000000000bb"]]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; New domain should receive FK
      (is (= {:domain/organization {:after [:organization/id #uuid "01010101-0000-0000-0000-000000000003"]}}
             (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-0000000000bb"])))
      ;; Old domain should be deleted
      (is (= {:domain/organization {:before [:organization/id #uuid "01010101-0000-0000-0000-000000000003"]
                                    :after :delete}}
             (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-0000000000aa"]))))))

(deftest process-ref-attributes-many-unchanged-items
  (testing "Items in both before and after are not touched"
    (let [delta {[:organization/id #uuid "01010101-0000-0000-0000-000000000004"]
                 {:organization/domains {:before [[:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000005"]
                                                  [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000006"]]
                                         :after [[:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000006"]
                                                 [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000007"]]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; d-1 removed → deleted
      (is (= {:domain/organization {:before [:organization/id #uuid "01010101-0000-0000-0000-000000000004"]
                                    :after :delete}}
             (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000005"])))
      ;; d-2 unchanged → no entry (or if entry, just FK set)
      ;; d-3 added → FK set
      (is (= {:domain/organization {:after [:organization/id #uuid "01010101-0000-0000-0000-000000000004"]}}
             (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000007"])))
      ;; d-2 should not be in the result (it's in both before and after)
      (is (nil? (get result [:domain/id #uuid "d0d0d0d0-0000-0000-0000-000000000006"]))))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest process-ref-attributes-empty-delta
  (testing "Empty delta returns empty result"
    (let [result (write/process-ref-attributes test-key->attribute {})]
      (is (= {} result)))))

(deftest process-ref-attributes-both-nil
  (testing "Both before and after nil - no processing"
    (let [delta {[:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000005"]
                 {:course/language-course {:before nil :after nil}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; The guard `(not (= before after nil))` should prevent processing
      ;; Delta should be unchanged
      (is (= delta result)))))

(deftest process-ref-attributes-multiple-entities
  (testing "Multiple entities in delta are all processed"
    (let [delta {[:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000006"]
                 {:course/language-course {:after [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000006"]}}
                 [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000007"]
                 {:course/language-course {:after [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000007"]}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Both courses should generate updates for their language-courses
      (is (= {:language-course/course {:after [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000006"]}}
             (get result [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000006"])))
      (is (= {:language-course/course {:after [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000007"]}}
             (get result [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000007"]))))))
