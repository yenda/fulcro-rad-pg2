(ns com.fulcrologic.rad.database-adapters.sql.save-form.integration-test
  "Integration tests for save-form! using PostgreSQL with isolated test schema.

   These tests verify that save-form! correctly persists form deltas to the database,
   using invariants to ensure correctness."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.write :as write]
   [com.fulcrologic.rad.database-adapters.sql.save-form.invariants :as inv]
   [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [next.jdbc.sql :as sql]
   [pg.pool :as pg.pool]
   [taoensso.encore :as enc]))

;; =============================================================================
;; Test Configuration
;; =============================================================================

(def test-db-config
  {:jdbcUrl "jdbc:postgresql://localhost:5432/fulcro-rad-pg2?user=user&password=password"})

(def pg2-config
  {:host "localhost"
   :port 5432
   :user "user"
   :password "password"
   :database "fulcro-rad-pg2"})

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def key->attribute (enc/keys-by ::attr/qualified-key attrs/all-attributes))

(defn generate-test-schema-name []
  (str "test_save_form_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn with-test-db
  "Run test function within an isolated test schema.
   Creates a unique schema per test and cleans up after."
  [f]
  (let [ds (jdbc/get-datasource test-db-config)
        schema-name (generate-test-schema-name)
        ;; Get JDBC connection for schema setup
        jdbc-conn (jdbc/get-connection ds)
        ;; Create pg2 pool with search_path set to test schema
        pg2-pool (pg.pool/pool (assoc pg2-config
                                      :pg-params {"search_path" schema-name}))]
    (try
      ;; Create isolated test schema using JDBC
      (jdbc/execute! jdbc-conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! jdbc-conn [(str "SET search_path TO " schema-name)])
      ;; Create tables using automatic schema generation
      (doseq [s (mig/automatic-schema :production attrs/all-attributes)]
        (jdbc/execute! jdbc-conn [s]))
      ;; Run the test with pg2 pool for writes, JDBC for reads/verification
      (let [env {::attr/key->attribute key->attribute
                 ::rad.sql/connection-pools {:production pg2-pool}}]
        (f jdbc-conn env))
      (finally
        ;; Clean up
        (pg.pool/close pg2-pool)
        (jdbc/execute! jdbc-conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close jdbc-conn)))))

;; =============================================================================
;; Helper Functions
;; =============================================================================

(def jdbc-opts {:builder-fn rs/as-unqualified-lower-maps})

(defn get-account [ds id]
  (first (jdbc/execute! ds ["SELECT * FROM accounts WHERE id = ?" id] jdbc-opts)))

(defn get-address [ds id]
  (first (jdbc/execute! ds ["SELECT * FROM addresses WHERE id = ?" id] jdbc-opts)))

(defn count-accounts [ds]
  (:count (first (jdbc/execute! ds ["SELECT COUNT(*) as count FROM accounts"] jdbc-opts))))

(defn count-addresses [ds]
  (:count (first (jdbc/execute! ds ["SELECT COUNT(*) as count FROM addresses"] jdbc-opts))))

;; =============================================================================
;; Basic Insert Tests
;; =============================================================================

(deftest test-insert-new-entity
  (testing "Inserting a new entity with tempid"
    (with-test-db
      (fn [ds env]
        (let [tempid (tempid/tempid)
              delta {[:account/id tempid] {:account/name {:after "Alice"}
                                           :account/active? {:after true}}}
              params {::rad.form/delta delta}

              ;; Execute save
              result (write/save-form! env params)

              ;; Verify tempid resolution invariant
              inv-result (inv/check-invariant
                          :tempid-resolution
                          {:delta delta :result result})]

          (is (:valid inv-result) (:reason inv-result))
          (is (= 1 (count (:tempids result))) "Should have one tempid mapped")

          ;; Verify data was persisted
          (let [real-id (get (:tempids result) tempid)
                account (get-account ds real-id)]
            (is (some? account) "Account should exist in database")
            (is (= "Alice" (:name account)))
            (is (= true (:active account)))))))))

(deftest test-insert-multiple-entities
  (testing "Inserting multiple new entities"
    (with-test-db
      (fn [ds env]
        (let [tempid1 (tempid/tempid)
              tempid2 (tempid/tempid)
              delta {[:account/id tempid1] {:account/name {:after "Alice"}}
                     [:account/id tempid2] {:account/name {:after "Bob"}}}
              params {::rad.form/delta delta}

              result (write/save-form! env params)

              ;; Verify invariants
              inv-result (inv/check-invariant
                          :tempids-are-unique
                          {:delta delta :result result})]

          (is (:valid inv-result) (:reason inv-result))
          (is (= 2 (count (:tempids result))))
          (is (= 2 (count-accounts ds))))))))

;; =============================================================================
;; Update Tests
;; =============================================================================

(deftest test-update-existing-entity
  (testing "Updating an existing entity"
    (with-test-db
      (fn [ds env]
        ;; Setup: create an account
        (let [account-id (ids/new-uuid)]
          (sql/insert! ds :accounts {:id account-id :name "Alice" :active true})

          ;; Update the account
          (let [delta {[:account/id account-id] {:account/name {:before "Alice" :after "Alice Updated"}
                                                 :account/active? {:before true :after false}}}
                params {::rad.form/delta delta}
                before-snapshot (inv/take-snapshot ds ["accounts"])

                result (write/save-form! env params)

                after-snapshot (inv/take-snapshot ds ["accounts"])
                diff (inv/diff-snapshots before-snapshot after-snapshot)]

            ;; Should have no new tempids for update
            (is (empty? (:tempids result)))

            ;; Verify the update
            (let [account (get-account ds account-id)]
              (is (= "Alice Updated" (:name account)))
              (is (= false (:active account))))))))))

(deftest test-idempotent-update
  (testing "Updating the same entity twice produces same result"
    (with-test-db
      (fn [ds env]
        (let [account-id (ids/new-uuid)]
          (sql/insert! ds :accounts {:id account-id :name "Alice" :active true})

          (let [delta {[:account/id account-id] {:account/name {:before "Alice" :after "Bob"}}}
                params {::rad.form/delta delta}]

            ;; First update
            (write/save-form! env params)
            (let [after-first (inv/take-snapshot ds ["accounts"])]

              ;; Second update (same delta)
              (write/save-form! env params)
              (let [after-second (inv/take-snapshot ds ["accounts"])

                    inv-result (inv/check-invariant-2
                                :idempotent-updates
                                after-first after-second {})]

                (is (:valid inv-result) (:reason inv-result))))))))))

;; =============================================================================
;; Delete Tests
;; =============================================================================

(deftest test-delete-entity
  (testing "Deleting an existing entity"
    (with-test-db
      (fn [ds env]
        (let [account-id (ids/new-uuid)]
          (sql/insert! ds :accounts {:id account-id :name "Alice"})
          (is (= 1 (count-accounts ds)))

          (let [delta {[:account/id account-id] {:delete true}}
                params {::rad.form/delta delta}]

            (write/save-form! env params)

            (is (= 0 (count-accounts ds)) "Account should be deleted")))))))

;; =============================================================================
;; Reference Tests
;; =============================================================================

(deftest test-insert-with-to-one-reference
  (testing "Insert entity with to-one reference"
    (with-test-db
      (fn [ds env]
        ;; Setup: create an address first
        (let [address-id (ids/new-uuid)]
          (sql/insert! ds :addresses {:id address-id :street "123 Main St"})

          ;; Create account with reference to address
          (let [account-tempid (tempid/tempid)
                delta {[:account/id account-tempid]
                       {:account/name {:after "Alice"}
                        :account/primary-address {:after [:address/id address-id]}}}
                params {::rad.form/delta delta}

                result (write/save-form! env params)
                real-account-id (get (:tempids result) account-tempid)
                account (get-account ds real-account-id)]

            (is (some? account))
            (is (= address-id (:primary_address account)))))))))

(deftest test-insert-with-new-to-one-reference
  (testing "Insert entity with reference to new entity (both tempids)"
    (with-test-db
      (fn [ds env]
        (let [account-tempid (tempid/tempid)
              address-tempid (tempid/tempid)

              ;; Both account and address are new
              delta {[:account/id account-tempid]
                     {:account/name {:after "Alice"}
                      :account/primary-address {:after [:address/id address-tempid]}}
                     [:address/id address-tempid]
                     {:address/street {:after "123 Main St"}
                      :address/city {:after "NYC"}}}
              params {::rad.form/delta delta}

              result (write/save-form! env params)]

          ;; Both tempids should be resolved
          (is (= 2 (count (:tempids result))))

          (let [real-account-id (get (:tempids result) account-tempid)
                real-address-id (get (:tempids result) address-tempid)
                account (get-account ds real-account-id)
                address (get-address ds real-address-id)]

            (is (some? account))
            (is (some? address))
            (is (= real-address-id (:primary_address account)))))))))

(deftest test-update-to-one-reference
  (testing "Update to-one reference to point to different entity"
    (with-test-db
      (fn [ds env]
        (let [account-id (ids/new-uuid)
              old-address-id (ids/new-uuid)
              new-address-id (ids/new-uuid)]

          ;; Setup
          (sql/insert! ds :addresses {:id old-address-id :street "Old St"})
          (sql/insert! ds :addresses {:id new-address-id :street "New St"})
          (sql/insert! ds :accounts {:id account-id :name "Alice" :primary_address old-address-id})

          ;; Update reference
          (let [delta {[:account/id account-id]
                       {:account/primary-address {:before [:address/id old-address-id]
                                                  :after [:address/id new-address-id]}}}
                params {::rad.form/delta delta}]

            (write/save-form! env params)

            (let [account (get-account ds account-id)]
              (is (= new-address-id (:primary_address account))))))))))

(deftest test-remove-to-one-reference
  (testing "Remove to-one reference (set to nil)"
    (with-test-db
      (fn [ds env]
        (let [account-id (ids/new-uuid)
              address-id (ids/new-uuid)]

          ;; Setup
          (sql/insert! ds :addresses {:id address-id :street "123 Main St"})
          (sql/insert! ds :accounts {:id account-id :name "Alice" :primary_address address-id})

          ;; Remove reference
          (let [delta {[:account/id account-id]
                       {:account/primary-address {:before [:address/id address-id]
                                                  :after nil}}}
                params {::rad.form/delta delta}]

            (write/save-form! env params)

            (let [account (get-account ds account-id)]
              (is (nil? (:primary_address account))))))))))

;; =============================================================================
;; Schema Isolation Tests
;; =============================================================================

(deftest test-schema-isolation
  (testing "Changes only affect tables in the relevant schema"
    (with-test-db
      (fn [ds env]
        ;; Add some initial data to both tables
        (let [existing-address-id (ids/new-uuid)]
          (sql/insert! ds :addresses {:id existing-address-id :street "Existing"})

          (let [before (inv/take-snapshot ds ["accounts" "addresses"])
                account-tempid (tempid/tempid)
                delta {[:account/id account-tempid] {:account/name {:after "New Account"}}}
                params {::rad.form/delta delta}

                _ (write/save-form! env params)

                after (inv/take-snapshot ds ["accounts" "addresses"])

                inv-result (inv/check-invariant-2
                            :schema-isolation
                            before after
                            {:expected-tables #{"accounts"}})]

            ;; Only accounts should change, not addresses
            (is (:valid inv-result) (:reason inv-result))))))))

;; =============================================================================
;; Enum Tests
;; =============================================================================

(deftest test-enum-value-persistence
  (testing "Enum values are persisted as strings"
    (with-test-db
      (fn [ds env]
        (let [address-tempid (tempid/tempid)
              delta {[:address/id address-tempid]
                     {:address/street {:after "123 Main St"}
                      :address/state {:after :state/AZ}}}
              params {::rad.form/delta delta}

              result (write/save-form! env params)
              real-id (get (:tempids result) address-tempid)
              address (get-address ds real-id)]

          (is (some? address))
          (is (= ":state/AZ" (:state address))))))))

;; =============================================================================
;; Verify-Save! Integration Test
;; =============================================================================

(deftest test-verify-save-integration
  (testing "Full verify-save! workflow"
    (with-test-db
      (fn [ds env]
        (let [tempid (tempid/tempid)
              delta {[:account/id tempid] {:account/name {:after "Test"}}}
              params {::rad.form/delta delta}

              result (inv/verify-save!
                      {:save-fn write/save-form!
                       :env env
                       :params params
                       :ds ds
                       :tables ["accounts"]
                       :invariants [:tempid-resolution :tempids-are-unique]})]

          (is (:valid result) (str "Invariants failed: "
                                   (pr-str (filter #(not (:valid %)) (:results result)))))
          (is (some? (:save-result result)))
          (is (nil? (:exception result))))))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest test-empty-delta
  (testing "Empty delta produces no changes"
    (with-test-db
      (fn [ds env]
        (let [before (inv/take-snapshot ds ["accounts" "addresses"])
              delta {}
              params {::rad.form/delta delta}

              result (write/save-form! env params)

              after (inv/take-snapshot ds ["accounts" "addresses"])]

          (is (= before after) "Empty delta should not change database")
          (is (empty? (:tempids result))))))))

(deftest test-no-change-delta
  (testing "Delta with no actual changes"
    (with-test-db
      (fn [ds env]
        (let [account-id (ids/new-uuid)]
          (sql/insert! ds :accounts {:id account-id :name "Alice"})

          (let [before (inv/take-snapshot ds ["accounts"])
                ;; Delta where before = after (no change)
                delta {[:account/id account-id] {:account/name {:before "Alice" :after "Alice"}}}
                params {::rad.form/delta delta}

                result (write/save-form! env params)

                after (inv/take-snapshot ds ["accounts"])]

            ;; No change should mean database unchanged
            ;; (Implementation may or may not issue UPDATE, but result should be same)
            (is (empty? (:tempids result)))))))))

(deftest test-null-value-handling
  (testing "Null values are properly handled"
    (with-test-db
      (fn [ds env]
        (let [account-id (ids/new-uuid)]
          (sql/insert! ds :accounts {:id account-id :name "Alice" :email "alice@example.com"})

          ;; Set email to nil
          (let [delta {[:account/id account-id]
                       {:account/email {:before "alice@example.com" :after nil}}}
                params {::rad.form/delta delta}]

            (write/save-form! env params)

            (let [account (get-account ds account-id)]
              (is (nil? (:email account))))))))))

;; =============================================================================
;; Edge Case Tests - Special Characters and Encoding
;; =============================================================================

(deftest test-sql-injection-protection
  (testing "SQL injection attempts are safely escaped via parameterized queries"
    (with-test-db
      (fn [ds env]
        (let [injection-strings ["Robert'); DROP TABLE accounts;--"
                                 "'; DELETE FROM accounts; --"
                                 "' OR '1'='1"
                                 "\" OR \"1\"=\"1"
                                 "1; UPDATE accounts SET name='hacked'"]]
          (doseq [evil-string injection-strings]
            (let [tempid (tempid/tempid)
                  delta {[:account/id tempid]
                         {:account/name {:after evil-string}}}
                  result (write/save-form! env {::rad.form/delta delta})
                  real-id (get (:tempids result) tempid)
                  account (get-account ds real-id)]
              (is (some? account) (str "Account should exist for: " evil-string))
              (is (= evil-string (:name account))
                  (str "Name should be stored literally: " evil-string)))))))))

(deftest test-unicode-handling
  (testing "Unicode characters are properly stored and retrieved"
    (with-test-db
      (fn [ds env]
        (let [unicode-strings ["日本語テスト" ;; Japanese
                               "Ümläüts" ;; German umlauts
                               "🎉 Emoji 🚀" ;; Emoji
                               "Привет мир" ;; Russian
                               "مرحبا" ;; Arabic
                               "中文测试"]] ;; Chinese
          (doseq [unicode-str unicode-strings]
            (let [tempid (tempid/tempid)
                  delta {[:account/id tempid]
                         {:account/name {:after unicode-str}}}
                  result (write/save-form! env {::rad.form/delta delta})
                  real-id (get (:tempids result) tempid)
                  account (get-account ds real-id)]
              (is (= unicode-str (:name account))
                  (str "Unicode should round-trip: " unicode-str)))))))))

(deftest test-special-characters-handling
  (testing "Special characters are properly stored"
    (with-test-db
      (fn [ds env]
        (let [special-strings ["O'Brien" ;; Single quote
                               "John \"The Boss\" Smith" ;; Double quotes
                               "Line1\nLine2" ;; Newline
                               "Tab\there" ;; Tab
                               "Back\\slash" ;; Backslash
                               "Semi;colon" ;; Semicolon
                               "   leading spaces"
                               "trailing spaces   "]]
          (doseq [special-str special-strings]
            (let [tempid (tempid/tempid)
                  delta {[:account/id tempid]
                         {:account/name {:after special-str}}}
                  result (write/save-form! env {::rad.form/delta delta})
                  real-id (get (:tempids result) tempid)
                  account (get-account ds real-id)]
              (is (= special-str (:name account))
                  (str "Special chars should round-trip: " (pr-str special-str))))))))))

(deftest test-empty-string-handling
  (testing "Empty strings are stored as empty strings (not null)"
    (with-test-db
      (fn [ds env]
        (let [tempid (tempid/tempid)
              delta {[:account/id tempid]
                     {:account/name {:after ""}}}
              result (write/save-form! env {::rad.form/delta delta})
              real-id (get (:tempids result) tempid)
              account (get-account ds real-id)]
          (is (= "" (:name account)) "Empty string should be preserved, not converted to null"))))))

;; =============================================================================
;; Edge Case Tests - Schema Violations (Expected Failures)
;; =============================================================================

(deftest test-oversized-string-rejected
  (testing "Strings exceeding column varchar limit throw wrapped save-error"
    (with-test-db
      (fn [ds env]
        (let [;; Test schema has varchar(200) for name
              oversized-string (apply str (repeat 201 "x"))
              tempid (tempid/tempid)
              delta {[:account/id tempid]
                     {:account/name {:after oversized-string}}}]
          (try
            (write/save-form! env {::rad.form/delta delta})
            (is false "Should have thrown")
            (catch clojure.lang.ExceptionInfo e
              (let [data (ex-data e)]
                (is (= ::write/save-error (:type data)))
                (is (= ::write/string-data-too-long (:cause data)))
                (is (= "22001" (:sql-state data)))
                (is (some? (:message data)))
                (is (instance? org.postgresql.util.PSQLException (ex-cause e)))))))))))

(deftest test-null-byte-rejected
  (testing "Null bytes in strings throw wrapped save-error"
    (with-test-db
      (fn [ds env]
        (let [string-with-null "Before\u0000After"
              tempid (tempid/tempid)
              delta {[:account/id tempid]
                     {:account/name {:after string-with-null}}}]
          (try
            (write/save-form! env {::rad.form/delta delta})
            (is false "Should have thrown")
            (catch clojure.lang.ExceptionInfo e
              (let [data (ex-data e)]
                (is (= ::write/save-error (:type data)))
                (is (= ::write/invalid-encoding (:cause data)))
                (is (= "22021" (:sql-state data)))
                (is (some? (:message data)))
                (is (instance? org.postgresql.util.PSQLException (ex-cause e)))))))))))

;; =============================================================================
;; Complex Mixed-Operation Test
;; =============================================================================

(deftest test-complex-mixed-operations-delta
  (testing "Complex delta with inserts, updates, deletes, and cross-references in single call"
    (with-test-db
      (fn [ds env]
        ;; SETUP: Create a rich initial state
        ;; - 3 existing accounts (one will be updated, one deleted, one left alone)
        ;; - 2 existing addresses (one will be updated, one referenced by new account)
        (let [;; Existing accounts
              alice-id (ids/new-uuid)
              bob-id (ids/new-uuid) ;; will be deleted
              charlie-id (ids/new-uuid) ;; left untouched
              ;; Existing addresses
              hq-address-id (ids/new-uuid) ;; will be updated
              branch-address-id (ids/new-uuid)] ;; will be referenced by new account]

          ;; Insert existing data
          (sql/insert! ds :accounts {:id alice-id :name "Alice" :email "alice@old.com" :active true})
          (sql/insert! ds :accounts {:id bob-id :name "Bob" :email "bob@delete.me" :active false})
          (sql/insert! ds :accounts {:id charlie-id :name "Charlie" :active true})
          (sql/insert! ds :addresses {:id hq-address-id :street "100 Old HQ" :city "OldCity" :state ":state/AZ"})
          (sql/insert! ds :addresses {:id branch-address-id :street "200 Branch St" :city "BranchTown"})

          ;; Verify initial state
          (is (= 3 (count-accounts ds)) "Should start with 3 accounts")
          (is (= 2 (count-addresses ds)) "Should start with 2 addresses")

          ;; BUILD THE MONSTER DELTA
          ;; This delta does ALL of the following in ONE save-form! call:
          ;; 1. INSERT: New account "Diana" with tempid
          ;; 2. INSERT: New account "Eve" with tempid, referencing NEW address
          ;; 3. INSERT: New address "Remote Office" with tempid (referenced by Eve)
          ;; 4. INSERT: New address "Warehouse" with tempid (standalone)
          ;; 5. UPDATE: Alice - change email and set inactive
          ;; 6. UPDATE: HQ address - change street and city
          ;; 7. DELETE: Bob's account
          ;; 8. UPDATE: Diana (new!) references existing branch-address

          (let [diana-tempid (tempid/tempid)
                eve-tempid (tempid/tempid)
                remote-office-tempid (tempid/tempid)
                warehouse-tempid (tempid/tempid)

                delta {;; INSERT: Diana (new account, references existing address)
                       [:account/id diana-tempid]
                       {:account/name {:after "Diana"}
                        :account/email {:after "diana@new.com"}
                        :account/active? {:after true}
                        :account/primary-address {:after [:address/id branch-address-id]}}

                       ;; INSERT: Eve (new account, references NEW address via tempid)
                       [:account/id eve-tempid]
                       {:account/name {:after "Eve"}
                        :account/email {:after "eve@new.com"}
                        :account/active? {:after false}
                        :account/primary-address {:after [:address/id remote-office-tempid]}}

                       ;; INSERT: Remote Office (new address, will be referenced by Eve)
                       [:address/id remote-office-tempid]
                       {:address/street {:after "999 Remote Rd"}
                        :address/city {:after "RemoteCity"}
                        :address/state {:after :state/KS}}

                       ;; INSERT: Warehouse (new standalone address)
                       [:address/id warehouse-tempid]
                       {:address/street {:after "500 Warehouse Way"}
                        :address/city {:after "StorageTown"}
                        :address/state {:after :state/MS}}

                       ;; UPDATE: Alice (change email, set inactive)
                       [:account/id alice-id]
                       {:account/email {:before "alice@old.com" :after "alice@new.com"}
                        :account/active? {:before true :after false}}

                       ;; UPDATE: HQ Address (change street and city)
                       [:address/id hq-address-id]
                       {:address/street {:before "100 Old HQ" :after "100 New HQ Blvd"}
                        :address/city {:before "OldCity" :after "NewCity"}}

                       ;; DELETE: Bob
                       [:account/id bob-id]
                       {:delete true}}

                params {::rad.form/delta delta}

                ;; EXECUTE
                result (write/save-form! env params)

                ;; Extract resolved tempids
                diana-real-id (get (:tempids result) diana-tempid)
                eve-real-id (get (:tempids result) eve-tempid)
                remote-office-real-id (get (:tempids result) remote-office-tempid)
                warehouse-real-id (get (:tempids result) warehouse-tempid)]

            ;; =================================================================
            ;; VERIFY TEMPID RESOLUTION
            ;; =================================================================
            (testing "Tempid resolution"
              (is (= 4 (count (:tempids result))) "Should resolve 4 tempids (2 accounts, 2 addresses)")
              (is (uuid? diana-real-id) "Diana should have real UUID")
              (is (uuid? eve-real-id) "Eve should have real UUID")
              (is (uuid? remote-office-real-id) "Remote office should have real UUID")
              (is (uuid? warehouse-real-id) "Warehouse should have real UUID")
              (is (= 4 (count (set [diana-real-id eve-real-id remote-office-real-id warehouse-real-id])))
                  "All resolved IDs should be unique"))

            ;; =================================================================
            ;; VERIFY INSERTS
            ;; =================================================================
            (testing "Insert: Diana (new account with reference to existing address)"
              (let [diana (get-account ds diana-real-id)]
                (is (some? diana) "Diana should exist")
                (is (= "Diana" (:name diana)))
                (is (= "diana@new.com" (:email diana)))
                (is (= true (:active diana)))
                (is (= branch-address-id (:primary_address diana))
                    "Diana should reference existing branch address")))

            (testing "Insert: Eve (new account with reference to NEW address)"
              (let [eve (get-account ds eve-real-id)]
                (is (some? eve) "Eve should exist")
                (is (= "Eve" (:name eve)))
                (is (= "eve@new.com" (:email eve)))
                (is (= false (:active eve)))
                (is (= remote-office-real-id (:primary_address eve))
                    "Eve should reference the new remote office address")))

            (testing "Insert: Remote Office address"
              (let [remote (get-address ds remote-office-real-id)]
                (is (some? remote) "Remote office should exist")
                (is (= "999 Remote Rd" (:street remote)))
                (is (= "RemoteCity" (:city remote)))
                (is (= ":state/KS" (:state remote)))))

            (testing "Insert: Warehouse address (standalone)"
              (let [warehouse (get-address ds warehouse-real-id)]
                (is (some? warehouse) "Warehouse should exist")
                (is (= "500 Warehouse Way" (:street warehouse)))
                (is (= "StorageTown" (:city warehouse)))
                (is (= ":state/MS" (:state warehouse)))))

            ;; =================================================================
            ;; VERIFY UPDATES
            ;; =================================================================
            (testing "Update: Alice (email and active status changed)"
              (let [alice (get-account ds alice-id)]
                (is (some? alice) "Alice should still exist")
                (is (= "Alice" (:name alice)) "Name should be unchanged")
                (is (= "alice@new.com" (:email alice)) "Email should be updated")
                (is (= false (:active alice)) "Active should be updated to false")))

            (testing "Update: HQ Address (street and city changed)"
              (let [hq (get-address ds hq-address-id)]
                (is (some? hq) "HQ address should still exist")
                (is (= "100 New HQ Blvd" (:street hq)) "Street should be updated")
                (is (= "NewCity" (:city hq)) "City should be updated")
                (is (= ":state/AZ" (:state hq)) "State should be unchanged")))

            ;; =================================================================
            ;; VERIFY DELETE
            ;; =================================================================
            (testing "Delete: Bob"
              (is (nil? (get-account ds bob-id)) "Bob should be deleted"))

            ;; =================================================================
            ;; VERIFY UNCHANGED ENTITIES
            ;; =================================================================
            (testing "Unchanged: Charlie (not in delta)"
              (let [charlie (get-account ds charlie-id)]
                (is (some? charlie) "Charlie should still exist")
                (is (= "Charlie" (:name charlie)))
                (is (= true (:active charlie)))))

            (testing "Unchanged: Branch address (only referenced, not modified)"
              (let [branch (get-address ds branch-address-id)]
                (is (some? branch) "Branch address should still exist")
                (is (= "200 Branch St" (:street branch)))
                (is (= "BranchTown" (:city branch)))))

            ;; =================================================================
            ;; VERIFY FINAL ROW COUNTS
            ;; =================================================================
            (testing "Final row counts"
              ;; Started: 3 accounts, +2 inserted, -1 deleted = 4
              (is (= 4 (count-accounts ds)) "Should end with 4 accounts")
              ;; Started: 2 addresses, +2 inserted = 4
              (is (= 4 (count-addresses ds)) "Should end with 4 addresses"))))))))
