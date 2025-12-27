(ns com.fulcrologic.rad.database-adapters.pg2.write-test
  "Unit tests for pure functions in write.clj.

   These tests verify the delta transformation logic without touching the database."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.pg2 :as rad.pg2]
   [com.fulcrologic.rad.database-adapters.pg2.write :as write])
  (:import
   [org.pg.error PGErrorResponse]
   [org.pg.msg.server ErrorResponse]))

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
    ::rad.pg2/fk-attr :language-course/course
    ::rad.pg2/delete-orphan? true}

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
    ::rad.pg2/fk-attr :domain/organization
    ::rad.pg2/delete-orphan? true}

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
    ::rad.pg2/fk-attr :address/account}

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
  (testing "Removing reference without delete-orphan? clears FK on target"
    (let [delta {[:account/id #uuid "a1a1a1a1-0000-0000-0000-000000000001"]
                 {:account/address {:before [:address/id #uuid "add1add1-0000-0000-0000-000000000001"]
                                    :after nil}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Target should have FK cleared (before set, after nil)
      (is (= {:address/account {:before [:account/id #uuid "a1a1a1a1-0000-0000-0000-000000000001"]
                                :after nil}}
             (get result [:address/id #uuid "add1add1-0000-0000-0000-000000000001"]))))))

(deftest process-ref-attributes-one-remove-ref-with-delete
  (testing "Removing reference with delete-orphan? marks target for deletion"
    (let [delta {[:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000002"]
                 {:course/language-course {:before [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000002"]
                                           :after nil}}}
          result (write/process-ref-attributes test-key->attribute delta)]
      ;; Target should be marked for deletion
      (is (= {:language-course/course {:before [:course/id #uuid "c1c1c1c1-0000-0000-0000-000000000002"]
                                       :after :delete}}
             (get result [:language-course/id #uuid "1c1c1c1c-0000-0000-0000-000000000002"]))))))

(deftest process-ref-attributes-one-change-ref-with-delete
  (testing "Changing reference (A→B) with delete-orphan? updates B and deletes A"
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
  (testing "Changing reference (A→B) without delete-orphan? updates B and clears A"
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
  (testing "Removing items from to-many with delete-orphan? marks them for deletion"
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

;; =============================================================================
;; resolve-tempid-in-value Tests
;; =============================================================================

(deftest resolve-tempid-in-value-scalar
  (testing "replaces tempid with real id"
    (let [tid (tempid/tempid)
          real-id #uuid "a1a1a1a1-0000-0000-0000-000000000001"
          tempids {tid real-id}]
      (is (= real-id (write/resolve-tempid-in-value tempids tid)))))

  (testing "returns value unchanged if not a tempid"
    (let [tempids {}]
      (is (= "hello" (write/resolve-tempid-in-value tempids "hello")))
      (is (= 42 (write/resolve-tempid-in-value tempids 42)))
      (is (= :keyword (write/resolve-tempid-in-value tempids :keyword)))))

  (testing "returns tempid unchanged if not in tempids map"
    (let [tid (tempid/tempid)
          tempids {}]
      (is (= tid (write/resolve-tempid-in-value tempids tid))))))

(deftest resolve-tempid-in-value-ident
  (testing "replaces tempid in ident"
    (let [tid (tempid/tempid)
          real-id #uuid "b2b2b2b2-0000-0000-0000-000000000002"
          tempids {tid real-id}
          ident [:user/id tid]]
      (is (= [:user/id real-id] (write/resolve-tempid-in-value tempids ident)))))

  (testing "leaves ident unchanged if id is not tempid"
    (let [existing-id #uuid "c3c3c3c3-0000-0000-0000-000000000003"
          tempids {}
          ident [:user/id existing-id]]
      (is (= ident (write/resolve-tempid-in-value tempids ident))))))

;; =============================================================================
;; plan-tempids Tests
;; =============================================================================

(def tempid-test-key->attribute
  {:user/id {::attr/qualified-key :user/id
             ::attr/type :uuid
             ::attr/identity? true
             ::rad.pg2/table "users"}
   :item/id {::attr/qualified-key :item/id
             ::attr/type :int
             ::attr/identity? true
             ::rad.pg2/table "items"}
   :event/id {::attr/qualified-key :event/id
              ::attr/type :long
              ::attr/identity? true
              ::rad.pg2/table "events"}})

(deftest plan-tempids-uuid-types
  (testing "UUID type tempids go to uuid-ids"
    (let [tid (tempid/tempid)
          delta {[:user/id tid] {:user/name {:after "Test User"}}}
          result (write/plan-tempids tempid-test-key->attribute delta)]
      (is (contains? (set (:uuid-ids result)) tid))
      (is (empty? (:sequence-ids result))))))

(deftest plan-tempids-int-types
  (testing "int type tempids go to sequence-ids"
    (let [tid (tempid/tempid)
          delta {[:item/id tid] {:item/name {:after "Test Item"}}}
          result (write/plan-tempids tempid-test-key->attribute delta)]
      (is (contains? (:sequence-ids result) tid))
      (is (= "items_id_seq" (get-in result [:sequence-ids tid :sequence])))
      (is (empty? (:uuid-ids result))))))

(deftest plan-tempids-long-types
  (testing "long type tempids go to sequence-ids"
    (let [tid (tempid/tempid)
          delta {[:event/id tid] {:event/type {:after "click"}}}
          result (write/plan-tempids tempid-test-key->attribute delta)]
      (is (contains? (:sequence-ids result) tid))
      (is (= "events_id_seq" (get-in result [:sequence-ids tid :sequence]))))))

(deftest plan-tempids-non-tempids-ignored
  (testing "existing IDs are not planned"
    (let [existing-id #uuid "d4d4d4d4-0000-0000-0000-000000000004"
          delta {[:user/id existing-id] {:user/name {:after "Existing User"}}}
          result (write/plan-tempids tempid-test-key->attribute delta)]
      (is (empty? (:uuid-ids result)))
      (is (empty? (:sequence-ids result))))))

(deftest plan-tempids-mixed
  (testing "mixed tempid types are properly categorized"
    (let [uuid-tid (tempid/tempid)
          int-tid (tempid/tempid)
          delta {[:user/id uuid-tid] {:user/name {:after "UUID User"}}
                 [:item/id int-tid] {:item/name {:after "Int Item"}}}
          result (write/plan-tempids tempid-test-key->attribute delta)]
      (is (contains? (set (:uuid-ids result)) uuid-tid))
      (is (contains? (:sequence-ids result) int-tid)))))

;; =============================================================================
;; resolve-uuid-tempids Tests
;; =============================================================================

(deftest resolve-uuid-tempids-test
  (testing "generates UUIDs for each tempid"
    (let [tid1 (tempid/tempid)
          tid2 (tempid/tempid)
          result (write/resolve-uuid-tempids [tid1 tid2])]
      (is (= 2 (count result)))
      (is (uuid? (get result tid1)))
      (is (uuid? (get result tid2)))
      (is (not= (get result tid1) (get result tid2)))))

  (testing "empty input returns empty map"
    (is (= {} (write/resolve-uuid-tempids []))))

  (testing "each call generates new UUIDs"
    (let [tid (tempid/tempid)
          result1 (write/resolve-uuid-tempids [tid])
          result2 (write/resolve-uuid-tempids [tid])]
      (is (not= (get result1 tid) (get result2 tid))))))

;; =============================================================================
;; form->sql-value Tests
;; =============================================================================

(deftest form->sql-value-ref-extracts-id
  (testing "extracts ID from ident for non-many refs"
    (let [attr {::attr/type :ref ::attr/cardinality :one}
          ident [:user/id #uuid "e5e5e5e5-0000-0000-0000-000000000005"]]
      (is (= #uuid "e5e5e5e5-0000-0000-0000-000000000005"
             (write/form->sql-value attr ident))))))

(deftest form->sql-value-custom-transformer
  (testing "uses custom form->sql-value transformer"
    (let [attr {::attr/type :string
                ::rad.pg2/form->sql-value str/upper-case}]
      (is (= "HELLO" (write/form->sql-value attr "hello"))))))

(deftest form->sql-value-keyword-encoding
  (testing "encodes keywords to SQL string"
    (let [attr {::attr/type :keyword}]
      (is (= ":status/active" (write/form->sql-value attr :status/active))))))

(deftest form->sql-value-passthrough
  (testing "passes through values without transformation"
    (let [string-attr {::attr/type :string}
          int-attr {::attr/type :int}
          bool-attr {::attr/type :boolean}]
      (is (= "test" (write/form->sql-value string-attr "test")))
      (is (= 42 (write/form->sql-value int-attr 42)))
      (is (= true (write/form->sql-value bool-attr true))))))

;; =============================================================================
;; keys-in-delta and schemas-for-delta Tests
;; =============================================================================

(deftest keys-in-delta-test
  (testing "extracts all keys from delta"
    (let [delta {[:user/id #uuid "f6f6f6f6-0000-0000-0000-000000000006"]
                 {:user/name {:after "Alice"}
                  :user/email {:after "alice@example.com"}}}
          result (write/keys-in-delta delta)]
      (is (contains? result :user/id))
      (is (contains? result :user/name))
      (is (contains? result :user/email))))

  (testing "extracts keys from multiple entities"
    (let [delta {[:user/id #uuid "00000000-0000-0000-0000-000000000001"]
                 {:user/name {:after "User 1"}}
                 [:order/id #uuid "00000000-0000-0000-0000-000000000002"]
                 {:order/total {:after 100}}}
          result (write/keys-in-delta delta)]
      (is (contains? result :user/id))
      (is (contains? result :user/name))
      (is (contains? result :order/id))
      (is (contains? result :order/total)))))

(deftest schemas-for-delta-test
  (testing "returns schemas for delta"
    (let [k->attr {:user/id {::attr/qualified-key :user/id ::attr/schema :production}
                   :user/name {::attr/qualified-key :user/name ::attr/schema :production}
                   :log/id {::attr/qualified-key :log/id ::attr/schema :logging}}
          env {::attr/key->attribute k->attr}
          delta {[:user/id #uuid "00000000-0000-0000-0000-000000000001"]
                 {:user/name {:after "Test"}}}
          result (write/schemas-for-delta env delta)]
      (is (contains? result :production)))))

;; =============================================================================
;; error-condition Tests
;; =============================================================================

;; Create PGErrorResponse with the desired error code for testing
(defn make-pg-error
  "Create a PGErrorResponse with the given error code for testing."
  [error-code]
  (let [err-map {"code" error-code
                 "message" "test error"
                 "severity" "ERROR"}]
    (PGErrorResponse. (ErrorResponse. err-map))))

(deftest error-condition-connection-error
  (testing "maps 08003 to connection-does-not-exist"
    (let [e (make-pg-error "08003")]
      (is (= ::write/connection-does-not-exist (write/error-condition e))))))

(deftest error-condition-data-errors
  (testing "maps 22001 to string-data-too-long"
    (let [e (make-pg-error "22001")]
      (is (= ::write/string-data-too-long (write/error-condition e)))))

  (testing "maps 22021 to invalid-encoding"
    (let [e (make-pg-error "22021")]
      (is (= ::write/invalid-encoding (write/error-condition e)))))

  (testing "maps 22P02 to invalid-text-representation"
    (let [e (make-pg-error "22P02")]
      (is (= ::write/invalid-text-representation (write/error-condition e))))))

(deftest error-condition-constraint-violations
  (testing "maps 23502 to not-null-violation"
    (let [e (make-pg-error "23502")]
      (is (= ::write/not-null-violation (write/error-condition e)))))

  (testing "maps 23505 to unique-violation"
    (let [e (make-pg-error "23505")]
      (is (= ::write/unique-violation (write/error-condition e)))))

  (testing "maps 23514 to check-violation"
    (let [e (make-pg-error "23514")]
      (is (= ::write/check-violation (write/error-condition e))))))

(deftest error-condition-transaction-errors
  (testing "maps 40001 to serialization-failure"
    (let [e (make-pg-error "40001")]
      (is (= ::write/serialization-failure (write/error-condition e)))))

  (testing "maps 57014 to timeout"
    (let [e (make-pg-error "57014")]
      (is (= ::write/timeout (write/error-condition e))))))

(deftest error-condition-unknown
  (testing "maps unknown codes to ::unknown"
    (let [e (make-pg-error "99999")]
      (is (= ::write/unknown (write/error-condition e))))))

;; =============================================================================
;; generate-insert Tests
;; =============================================================================

(def insert-test-key->attribute
  {:account/id {::attr/qualified-key :account/id
                ::attr/type :uuid
                ::attr/identity? true
                ::attr/schema :production
                ::rad.pg2/table "accounts"}
   :account/name {::attr/qualified-key :account/name
                  ::attr/type :string
                  ::attr/schema :production
                  ::attr/identities #{:account/id}}
   :account/email {::attr/qualified-key :account/email
                   ::attr/type :string
                   ::attr/schema :production
                   ::attr/identities #{:account/id}}
   :account/active {::attr/qualified-key :account/active
                    ::attr/type :boolean
                    ::attr/schema :production
                    ::attr/identities #{:account/id}}})

(deftest generate-insert-creates-valid-sql
  (testing "generates INSERT for new entity"
    (let [tid (tempid/tempid)
          real-id #uuid "a1a1a1a1-1111-1111-1111-111111111111"
          tempids {tid real-id}
          ident [:account/id tid]
          diff {:account/name {:after "Test Account"}
                :account/email {:after "test@example.com"}}
          result (write/generate-insert insert-test-key->attribute :production tempids ident diff)]
      (is (vector? result))
      (is (string? (first result)))
      (is (str/includes? (first result) "INSERT INTO"))
      (is (str/includes? (first result) "accounts")))))

(deftest generate-insert-returns-nil-for-existing-id
  (testing "returns nil for existing (non-tempid) entity"
    (let [existing-id #uuid "b2b2b2b2-2222-2222-2222-222222222222"
          ident [:account/id existing-id]
          diff {:account/name {:after "Existing"}}
          result (write/generate-insert insert-test-key->attribute :production {} ident diff)]
      (is (nil? result)))))

(deftest generate-insert-returns-nil-for-wrong-schema
  (testing "returns nil for wrong schema"
    (let [tid (tempid/tempid)
          real-id #uuid "c3c3c3c3-3333-3333-3333-333333333333"
          tempids {tid real-id}
          ident [:account/id tid]
          diff {:account/name {:after "Wrong Schema"}}
          result (write/generate-insert insert-test-key->attribute :other-schema tempids ident diff)]
      (is (nil? result)))))

;; =============================================================================
;; generate-update Tests
;; =============================================================================

(deftest generate-update-creates-valid-sql
  (testing "generates UPDATE for existing entity"
    (let [existing-id #uuid "d4d4d4d4-4444-4444-4444-444444444444"
          ident [:account/id existing-id]
          diff {:account/name {:before "Old Name" :after "New Name"}}
          result (write/generate-update insert-test-key->attribute :production {} ident diff)]
      (is (vector? result))
      (is (string? (first result)))
      (is (str/includes? (first result) "UPDATE"))
      (is (str/includes? (first result) "accounts")))))

(deftest generate-update-returns-nil-for-tempid
  (testing "returns nil for tempid entity (handled by insert)"
    (let [tid (tempid/tempid)
          ident [:account/id tid]
          diff {:account/name {:after "New"}}
          result (write/generate-update insert-test-key->attribute :production {} ident diff)]
      (is (nil? result)))))

(deftest generate-update-generates-delete
  (testing "generates DELETE when :delete is in diff"
    (let [existing-id #uuid "e5e5e5e5-5555-5555-5555-555555555555"
          ident [:account/id existing-id]
          diff {:delete true}
          result (write/generate-update insert-test-key->attribute :production {} ident diff)]
      (is (vector? result))
      (is (str/includes? (first result) "DELETE FROM")))))

(deftest generate-update-clears-null-values
  (testing "sets column to NULL when after is nil"
    (let [existing-id #uuid "f6f6f6f6-6666-6666-6666-666666666666"
          ident [:account/id existing-id]
          diff {:account/email {:before "old@example.com" :after nil}}
          result (write/generate-update insert-test-key->attribute :production {} ident diff)]
      (is (vector? result))
      (is (string? (first result))))))

;; =============================================================================
;; delta->sql-plan Tests
;; =============================================================================

(deftest delta->sql-plan-separates-inserts-and-updates
  (testing "creates separate inserts and updates"
    (let [tid (tempid/tempid)
          existing-id #uuid "00000000-0000-0000-0000-000000000007"
          real-id #uuid "00000000-0000-0000-0000-000000000008"
          tempids {tid real-id}
          delta {[:account/id tid] {:account/name {:after "New Account"}}
                 [:account/id existing-id] {:account/name {:before "Old" :after "Updated"}}}
          result (write/delta->sql-plan insert-test-key->attribute :production tempids delta)]
      (is (map? result))
      (is (vector? (:inserts result)))
      (is (vector? (:updates result)))
      (is (= 1 (count (:inserts result))))
      (is (= 1 (count (:updates result)))))))

(deftest delta->sql-plan-handles-empty-delta
  (testing "returns empty vectors for empty delta"
    (let [result (write/delta->sql-plan insert-test-key->attribute :production {} {})]
      (is (= {:inserts [] :updates []} result)))))

(deftest delta->sql-plan-filters-by-schema
  (testing "only includes statements for matching schema"
    (let [tid (tempid/tempid)
          real-id #uuid "00000000-0000-0000-0000-000000000009"
          tempids {tid real-id}
          delta {[:account/id tid] {:account/name {:after "Account"}}}
          ;; Query for wrong schema
          result (write/delta->sql-plan insert-test-key->attribute :wrong-schema tempids delta)]
      (is (empty? (:inserts result)))
      (is (empty? (:updates result))))))

;; =============================================================================
;; entity->table-row Tests (used by save-entity!)
;; =============================================================================

(def entity-test-key->attribute
  "Key->attribute map for testing entity->table-row with refs."
  {:account/id {::attr/qualified-key :account/id
                ::attr/type :uuid
                ::attr/identity? true
                ::attr/schema :production
                ::rad.pg2/table "accounts"}
   :account/name {::attr/qualified-key :account/name
                  ::attr/type :string
                  ::attr/schema :production
                  ::attr/identities #{:account/id}}
   :account/address {::attr/qualified-key :account/address
                     ::attr/type :ref
                     ::attr/cardinality :one
                     ::attr/target :address/id
                     ::attr/schema :production
                     ::attr/identities #{:account/id}
                     ::rad.pg2/column-name "address_id"}
   :address/id {::attr/qualified-key :address/id
                ::attr/type :uuid
                ::attr/identity? true
                ::attr/schema :production
                ::rad.pg2/table "addresses"}})

(deftest entity->table-row-handles-ref-with-ident-format
  (testing "extracts ID from ident format ref value"
    (let [account-id #uuid "11111111-1111-1111-1111-111111111111"
          address-id #uuid "22222222-2222-2222-2222-222222222222"
          entity {:account/id account-id
                  :account/name "Test Account"
                  :account/address [:address/id address-id]}
          result (#'write/entity->table-row entity-test-key->attribute entity)]
      (is (= :accounts (:table result)))
      (is (= account-id (get-in result [:row :id])))
      (is (= "Test Account" (get-in result [:row :name])))
      ;; Key assertion: ref value should be extracted from ident
      (is (= address-id (get-in result [:row :address_id])))))

  (testing "handles direct UUID for ref (not ident format)"
    (let [account-id #uuid "33333333-3333-3333-3333-333333333333"
          address-id #uuid "44444444-4444-4444-4444-444444444444"
          entity {:account/id account-id
                  :account/name "Direct UUID"
                  :account/address address-id}
          result (#'write/entity->table-row entity-test-key->attribute entity)]
      (is (= :accounts (:table result)))
      ;; Direct UUID should pass through unchanged
      (is (= address-id (get-in result [:row :address_id])))))

  (testing "handles nil ref value"
    (let [account-id #uuid "55555555-5555-5555-5555-555555555555"
          entity {:account/id account-id
                  :account/name "No Address"
                  :account/address nil}
          result (#'write/entity->table-row entity-test-key->attribute entity)]
      (is (= :accounts (:table result)))
      ;; nil should not be included (encode-for-sql returns nil for nil)
      (is (nil? (get-in result [:row :address_id])))))

  (testing "handles map format ref value (e.g., {:address/id uuid})"
    (let [account-id #uuid "66666666-6666-6666-6666-666666666666"
          address-id #uuid "77777777-7777-7777-7777-777777777777"
          entity {:account/id account-id
                  :account/name "Map Format"
                  :account/address {:address/id address-id}}
          result (#'write/entity->table-row entity-test-key->attribute entity)]
      (is (= :accounts (:table result)))
      ;; Map format should extract the ID value
      (is (= address-id (get-in result [:row :address_id])))))

  (testing "returns id-col for ON CONFLICT"
    (let [account-id #uuid "88888888-8888-8888-8888-888888888888"
          entity {:account/id account-id
                  :account/name "With ID Col"}
          result (#'write/entity->table-row entity-test-key->attribute entity)]
      (is (= :id (:id-col result))))))

;; Regression test for bug where save-entity! picked wrong ON CONFLICT column
;; when table had multiple *_id columns (e.g., owner_id, forked_from_id)
(def sidekick-key->attribute
  "Key->attribute map simulating sidekicks table with multiple *_id columns."
  {:sidekick/id {::attr/qualified-key :sidekick/id
                 ::attr/type :uuid
                 ::attr/identity? true
                 ::attr/schema :production
                 ::rad.pg2/table "sidekicks"}
   :sidekick/name {::attr/qualified-key :sidekick/name
                   ::attr/type :string
                   ::attr/schema :production
                   ::attr/identities #{:sidekick/id}}
   :sidekick/owner {::attr/qualified-key :sidekick/owner
                    ::attr/type :ref
                    ::attr/cardinality :one
                    ::attr/target :user/id
                    ::attr/schema :production
                    ::attr/identities #{:sidekick/id}
                    ::rad.pg2/column-name "owner_id"}
   :sidekick/forked-from {::attr/qualified-key :sidekick/forked-from
                          ::attr/type :ref
                          ::attr/cardinality :one
                          ::attr/target :sidekick/id
                          ::attr/schema :production
                          ::attr/identities #{:sidekick/id}
                          ::rad.pg2/column-name "forked_from_id"}
   :user/id {::attr/qualified-key :user/id
             ::attr/type :uuid
             ::attr/identity? true
             ::attr/schema :production
             ::rad.pg2/table "users"}})

(deftest entity->table-row-uses-identity-column-not-first-id-column
  (testing "id-col is identity column, not first *_id column alphabetically"
    ;; This tests the bug where (first (filter #(str/ends-with? "_id") ...))
    ;; would pick owner_id or forked_from_id instead of id
    (let [sidekick-id #uuid "99999999-9999-9999-9999-999999999999"
          owner-id #uuid "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
          entity {:sidekick/id sidekick-id
                  :sidekick/name "Test Sidekick"
                  :sidekick/owner owner-id}
          result (#'write/entity->table-row sidekick-key->attribute entity)]
      ;; The identity column should be :id, not :owner_id or :forked_from_id
      (is (= :id (:id-col result))
          "id-col should be the identity column :id, not a ref column like :owner_id")))

  (testing "entity with nil owner still uses correct id-col"
    (let [sidekick-id #uuid "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
          entity {:sidekick/id sidekick-id
                  :sidekick/name "System Sidekick"
                  :sidekick/owner nil}
          result (#'write/entity->table-row sidekick-key->attribute entity)]
      (is (= :id (:id-col result))))))
