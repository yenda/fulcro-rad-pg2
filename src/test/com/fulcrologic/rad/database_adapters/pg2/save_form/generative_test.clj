(ns com.fulcrologic.rad.database-adapters.pg2.save-form.generative-test
  "Generative (property-based) tests for save-form!

   Uses test.check to generate random deltas and verify properties hold."
  (:require
   [clojure.test :refer [is]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.pg2 :as rad.pg2]
   [com.fulcrologic.rad.database-adapters.pg2.migration :as mig]
   [com.fulcrologic.rad.database-adapters.pg2.write :as write]
   [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
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

(def key->attribute (enc/keys-by ::attr/qualified-key attrs/all-attributes))

;; =============================================================================
;; Generators - Derived from Test Attributes
;; =============================================================================

(def gen-tempid
  "Generator for Fulcro tempids"
  (gen/fmap (fn [_] (tempid/tempid)) gen/nat))

;; Problematic strings that could break SQL or encoding
(def sql-injection-strings
  ["Robert'); DROP TABLE accounts;--"
   "'; DELETE FROM accounts; --"
   "1; UPDATE accounts SET name='hacked'"
   "' OR '1'='1"
   "\" OR \"1\"=\"1"])

(def special-char-strings
  ["O'Brien" ;; Single quote - SQL escape testing
   "John \"The Boss\" Smith" ;; Double quotes
   "Line1\nLine2" ;; Newline
   "Tab\there" ;; Tab
   "Back\\slash" ;; Backslash
   "Semi;colon" ;; Semicolon
   "Percent%sign" ;; Percent (SQL LIKE wildcard)
   "Under_score" ;; Underscore (SQL LIKE wildcard)
   "日本語テスト" ;; Japanese
   "Ümläüts" ;; Umlauts
   "🎉 Emoji 🚀" ;; Emoji
   ;; Note: \u0000 null bytes are rejected by PostgreSQL - not testing
   "   leading spaces"
   "trailing spaces   "
   "  both  spaces  "])

(def boundary-strings
  ;; Note: varchar(200) in test schema, so staying under that limit
  ["" ;; Empty string
   "x" ;; Single char
   (apply str (repeat 199 "x")) ;; Just under limit
   (apply str (repeat 200 "x"))]) ;; At limit

(def gen-nasty-string
  "Generator for strings that might break things"
  (gen/one-of [(gen/elements sql-injection-strings)
               (gen/elements special-char-strings)
               (gen/elements boundary-strings)]))

(def gen-nice-string
  "Generator for normal alphanumeric strings"
  (gen/such-that #(and (seq %) (<= (count %) 200))
                 gen/string-alphanumeric))

(def gen-string-value
  "Generator for string field values - mixes nice and nasty"
  (gen/frequency [[7 gen-nice-string] ;; 70% normal strings
                  [3 gen-nasty-string]])) ;; 30% problematic strings

(def gen-boolean-value
  "Generator for boolean field values"
  gen/boolean)

(def gen-email-value
  "Generator for email-like strings - includes edge cases"
  (gen/frequency
   [[7 (gen/fmap (fn [[name domain]]
                   (str name "@" domain ".com"))
                 (gen/tuple gen-nice-string gen-nice-string))]
    [1 (gen/return "")] ;; Empty email
    [1 (gen/return "not-an-email")] ;; Invalid format
    [1 (gen/return "test@evil.com'; DROP TABLE--")] ;; SQL injection in email
    ]))

(def gen-state-enum
  "Generator for state enum values (from test attributes)"
  (gen/elements [:state/AZ :state/KS :state/MS]))

;; =============================================================================
;; Delta Generators
;; =============================================================================

(defn gen-account-insert
  "Generator for an account insert delta entry"
  []
  (gen/let [tempid gen-tempid
            name gen-string-value
            include-active? gen/boolean
            active? gen-boolean-value
            include-email? gen/boolean
            email gen-email-value]
    (let [base-fields {:account/name {:after name}}
          fields (cond-> base-fields
                   include-active? (assoc :account/active? {:after active?})
                   include-email? (assoc :account/email {:after email}))]
      {[:account/id tempid] fields})))

(defn gen-address-insert
  "Generator for an address insert delta entry"
  []
  (gen/let [tempid gen-tempid
            street gen-string-value
            include-city? gen/boolean
            city gen-string-value
            include-state? gen/boolean
            state gen-state-enum]
    (let [base-fields {:address/street {:after street}}
          fields (cond-> base-fields
                   include-city? (assoc :address/city {:after city})
                   include-state? (assoc :address/state {:after state}))]
      {[:address/id tempid] fields})))

(def gen-single-insert-delta
  "Generator for a delta with a single insert"
  (gen/one-of [(gen-account-insert)
               (gen-address-insert)]))

(def gen-multi-insert-delta
  "Generator for a delta with multiple inserts"
  (gen/let [num-accounts (gen/choose 1 3)
            num-addresses (gen/choose 0 2)
            account-inserts (gen/vector (gen-account-insert) num-accounts)
            address-inserts (gen/vector (gen-address-insert) num-addresses)]
    (apply merge (concat account-inserts address-inserts))))

(def gen-insert-with-reference-delta
  "Generator for inserting account with reference to new address"
  (gen/let [account-tempid gen-tempid
            address-tempid gen-tempid
            account-name gen-string-value
            street gen-string-value]
    {[:account/id account-tempid]
     {:account/name {:after account-name}
      :account/primary-address {:after [:address/id address-tempid]}}
     [:address/id address-tempid]
     {:address/street {:after street}}}))

;; =============================================================================
;; Test Fixture
;; =============================================================================

(defn generate-test-schema-name []
  (str "test_gen_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn with-test-db
  "Run function with isolated test database schema"
  [f]
  (let [ds (jdbc/get-datasource test-db-config)
        schema-name (generate-test-schema-name)
        jdbc-conn (jdbc/get-connection ds)
        ;; Create pg2 pool with search_path set to test schema
        pg2-pool (pg.pool/pool (assoc pg2-config
                                      :pg-params {"search_path" schema-name}))]
    (try
      ;; Use JDBC for schema setup
      (jdbc/execute! jdbc-conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! jdbc-conn [(str "SET search_path TO " schema-name)])
      (doseq [s (mig/automatic-schema :production attrs/all-attributes)]
        (jdbc/execute! jdbc-conn [s]))
      ;; Use pg2 pool for actual operations
      (let [env {::attr/key->attribute key->attribute
                 ::rad.pg2/connection-pools {:production pg2-pool}}]
        (f jdbc-conn env))
      (finally
        (pg.pool/close pg2-pool)
        (jdbc/execute! jdbc-conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close jdbc-conn)))))

(def jdbc-opts {:builder-fn rs/as-unqualified-lower-maps})

(defn get-account [conn id]
  (first (jdbc/execute! conn ["SELECT * FROM accounts WHERE id = ?" id] jdbc-opts)))

(defn get-address [conn id]
  (first (jdbc/execute! conn ["SELECT * FROM addresses WHERE id = ?" id] jdbc-opts)))

(defn count-rows [conn table]
  (:count (first (jdbc/execute! conn [(str "SELECT COUNT(*) as count FROM " table)] jdbc-opts))))

;; =============================================================================
;; Properties
;; =============================================================================

(defn prop-tempids-resolved
  "Property: All tempids in delta are resolved to real UUIDs"
  [delta result]
  (let [tempids-in-delta (->> (keys delta)
                              (map second)
                              (filter tempid/tempid?)
                              set)
        resolved-tempids (set (keys (:tempids result)))]
    (= tempids-in-delta resolved-tempids)))

(defn prop-tempids-unique
  "Property: All resolved IDs are unique"
  [result]
  (let [real-ids (vals (:tempids result))]
    (= (count real-ids) (count (set real-ids)))))

(defn prop-data-persisted
  "Property: Inserted data can be read back"
  [conn delta result]
  (every?
   (fn [[[id-key id] _fields]]
     (when (tempid/tempid? id)
       (let [real-id (get (:tempids result) id)
             table-name (namespace id-key)]
         (case table-name
           "account" (some? (get-account conn real-id))
           "address" (some? (get-address conn real-id))
           true))))
   delta))

;; =============================================================================
;; Property-Based Tests
;; =============================================================================

(defspec single-insert-resolves-tempids 50
  (prop/for-all [delta gen-single-insert-delta]
                (with-test-db
                  (fn [_conn env]
                    (let [result (write/save-form! env {::rad.form/delta delta})]
                      (and (prop-tempids-resolved delta result)
                           (prop-tempids-unique result)))))))

(defspec multi-insert-resolves-all-tempids 30
  (prop/for-all [delta gen-multi-insert-delta]
                (with-test-db
                  (fn [_conn env]
                    (let [result (write/save-form! env {::rad.form/delta delta})]
                      (and (prop-tempids-resolved delta result)
                           (prop-tempids-unique result)))))))

(defspec insert-persists-data 30
  (prop/for-all [delta gen-single-insert-delta]
                (with-test-db
                  (fn [conn env]
                    (let [result (write/save-form! env {::rad.form/delta delta})]
                      (prop-data-persisted conn delta result))))))

(defspec insert-with-reference-works 20
  (prop/for-all [delta gen-insert-with-reference-delta]
                (with-test-db
                  (fn [conn env]
                    (let [result (write/save-form! env {::rad.form/delta delta})
                          ;; Find the account and address tempids
                          account-entry (first (filter #(= "account" (namespace (ffirst %))) delta))
                          address-entry (first (filter #(= "address" (namespace (ffirst %))) delta))
                          account-tempid (second (first account-entry))
                          address-tempid (second (first address-entry))
                          real-account-id (get (:tempids result) account-tempid)
                          real-address-id (get (:tempids result) address-tempid)
                          account (get-account conn real-account-id)]
                      ;; Account's primary_address should point to the address
                      (= real-address-id (:primary_address account)))))))

(defspec empty-delta-is-noop 20
  (prop/for-all [_ gen/nat]
                (with-test-db
                  (fn [conn env]
                    (let [before-accounts (count-rows conn "accounts")
                          before-addresses (count-rows conn "addresses")
                          result (write/save-form! env {::rad.form/delta {}})
                          after-accounts (count-rows conn "accounts")
                          after-addresses (count-rows conn "addresses")]
                      (and (empty? (:tempids result))
                           (= before-accounts after-accounts)
                           (= before-addresses after-addresses)))))))

(defspec updates-are-idempotent 20
  (prop/for-all [insert-delta gen-single-insert-delta]
                (with-test-db
                  (fn [conn env]
                    ;; First insert an entity
                    (let [insert-result (write/save-form! env {::rad.form/delta insert-delta})
                          ;; Get the first account tempid and its real id
                          account-entries (filter #(= "account" (namespace (ffirst %))) insert-delta)]
                      (if (empty? account-entries)
                        true ;; Skip if no accounts in delta
                        (let [account-tempid (second (first (first account-entries)))
                              real-id (get (:tempids insert-result) account-tempid)
                              ;; Create an update delta
                              update-delta {[:account/id real-id] {:account/name {:before "old" :after "new-name"}}}
                              ;; Apply update twice
                              _ (write/save-form! env {::rad.form/delta update-delta})
                              after-first (get-account conn real-id)
                              _ (write/save-form! env {::rad.form/delta update-delta})
                              after-second (get-account conn real-id)]
                          ;; Both should have same state
                          (= after-first after-second))))))))

(defspec delete-removes-entity 20
  (prop/for-all [_ gen/nat]
                (with-test-db
                  (fn [conn env]
                    ;; Insert an account first
                    (let [tempid (tempid/tempid)
                          insert-delta {[:account/id tempid] {:account/name {:after "ToDelete"}}}
                          insert-result (write/save-form! env {::rad.form/delta insert-delta})
                          real-id (get (:tempids insert-result) tempid)
                          _ (is (some? (get-account conn real-id)) "Account should exist after insert")
                          ;; Delete it
                          delete-delta {[:account/id real-id] {:delete true}}
                          _ (write/save-form! env {::rad.form/delta delete-delta})]
                      (nil? (get-account conn real-id)))))))

;; =============================================================================
;; Additional Complex Scenarios
;; =============================================================================

(defspec multiple-operations-single-delta 15
  (prop/for-all [num-inserts (gen/choose 1 5)]
                (with-test-db
                  (fn [conn env]
                    ;; Generate multiple account inserts
                    (let [tempids (repeatedly num-inserts tempid/tempid)
                          delta (into {}
                                      (map-indexed
                                       (fn [i tid]
                                         [[:account/id tid]
                                          {:account/name {:after (str "Account" i)}}])
                                       tempids))
                          result (write/save-form! env {::rad.form/delta delta})]
                      ;; All tempids should be resolved
                      (and (= num-inserts (count (:tempids result)))
                           (= num-inserts (count-rows conn "accounts"))))))))

;; =============================================================================
;; Complex Mixed-Operation Scenarios
;; =============================================================================

;; Tests a complex delta that mixes inserts, updates, deletes, and references
;; in a single save-form! call. This simulates realistic form submissions where
;; multiple related entities are modified together.
(defspec mixed-operations-complex-delta 20
  (prop/for-all [;; Generate random field values
                 new-account-name gen-string-value
                 new-address-street gen-string-value
                 new-address-city gen-string-value
                 updated-name gen-string-value
                 updated-email gen-email-value
                 include-update-email? gen/boolean
                 include-new-address-ref? gen/boolean]
                (with-test-db
                  (fn [conn env]
                    ;; SETUP: Create existing entities to update/delete/reference
                    (let [existing-account-1-id (ids/new-uuid)
                          existing-account-2-id (ids/new-uuid) ;; will be deleted
                          existing-address-id (ids/new-uuid)]

                      ;; Insert existing data directly
                      (jdbc/execute! conn
                                     ["INSERT INTO accounts (id, name, email, active) VALUES (?, ?, ?, ?)"
                                      existing-account-1-id "ExistingAccount1" "existing@test.com" true])
                      (jdbc/execute! conn
                                     ["INSERT INTO accounts (id, name, active) VALUES (?, ?, ?)"
                                      existing-account-2-id "ToBeDeleted" false])
                      (jdbc/execute! conn
                                     ["INSERT INTO addresses (id, street, city) VALUES (?, ?, ?)"
                                      existing-address-id "Old Street" "Old City"])

                      ;; BUILD COMPLEX DELTA mixing all operation types:
                      ;; 1. Insert new account (with tempid)
                      ;; 2. Insert new address (with tempid)
                      ;; 3. Update existing account (change name, optionally email)
                      ;; 4. Delete existing account
                      ;; 5. Optionally: new account references new address
                      (let [new-account-tempid (tempid/tempid)
                            new-address-tempid (tempid/tempid)

                            ;; Build the mixed delta
                            delta (cond->
                                   {;; INSERT: new account
                                    [:account/id new-account-tempid]
                                    (cond-> {:account/name {:after new-account-name}
                                             :account/active? {:after true}}
                                      include-new-address-ref?
                                      (assoc :account/primary-address
                                             {:after [:address/id new-address-tempid]}))

                                    ;; INSERT: new address
                                    [:address/id new-address-tempid]
                                    {:address/street {:after new-address-street}
                                     :address/city {:after new-address-city}
                                     :address/state {:after :state/AZ}}

                                    ;; UPDATE: existing account
                                    [:account/id existing-account-1-id]
                                    (cond-> {:account/name {:before "ExistingAccount1"
                                                            :after updated-name}}
                                      include-update-email?
                                      (assoc :account/email {:before "existing@test.com"
                                                             :after updated-email}))

                                    ;; DELETE: existing account
                                    [:account/id existing-account-2-id]
                                    {:delete true}}

                                    ;; Always include the new address in delta
                                    true identity)

                            ;; Execute the complex save
                            result (write/save-form! env {::rad.form/delta delta})

                            ;; Verify all operations succeeded
                            new-account-real-id (get (:tempids result) new-account-tempid)
                            new-address-real-id (get (:tempids result) new-address-tempid)]

                        (and
                         ;; Tempids resolved correctly (2 new entities)
                         (= 2 (count (:tempids result)))
                         (uuid? new-account-real-id)
                         (uuid? new-address-real-id)

                         ;; INSERT verified: new account exists with correct data
                         (let [new-account (get-account conn new-account-real-id)]
                           (and (some? new-account)
                                (= new-account-name (:name new-account))
                                (= true (:active new-account))
                                (if include-new-address-ref?
                                  (= new-address-real-id (:primary_address new-account))
                                  true)))

                         ;; INSERT verified: new address exists with correct data
                         (let [new-address (get-address conn new-address-real-id)]
                           (and (some? new-address)
                                (= new-address-street (:street new-address))
                                (= new-address-city (:city new-address))
                                (= ":state/AZ" (:state new-address))))

                         ;; UPDATE verified: existing account has new values
                         (let [updated-account (get-account conn existing-account-1-id)]
                           (and (some? updated-account)
                                (= updated-name (:name updated-account))
                                (if include-update-email?
                                  (= updated-email (:email updated-account))
                                  true)))

                         ;; DELETE verified: deleted account is gone
                         (nil? (get-account conn existing-account-2-id))

                         ;; Existing address unchanged
                         (let [existing-addr (get-address conn existing-address-id)]
                           (and (some? existing-addr)
                                (= "Old Street" (:street existing-addr))
                                (= "Old City" (:city existing-addr))))

                         ;; Row counts are correct:
                         ;; Started with 2 accounts, added 1, deleted 1 = 2
                         ;; Started with 1 address, added 1 = 2
                         (= 2 (count-rows conn "accounts"))
                         (= 2 (count-rows conn "addresses")))))))))

;; =============================================================================
;; Chaos Mode - Invalid Data Generators
;; =============================================================================
;; These generators produce data that violates database constraints.
;; Used to verify that save-form! properly wraps errors.

;; NOTE: gen-null-byte-string removed - pg2 driver handles null bytes differently
;; than JDBC. JDBC would throw error 22021 (invalid_byte_sequence_for_encoding),
;; but pg2 successfully inserts strings with embedded null bytes.

(def gen-oversized-string
  "Generator for strings exceeding varchar(200) limit"
  (gen/fmap (fn [n]
              (apply str (repeat (+ 201 n) "x")))
            (gen/choose 0 100)))

(def gen-chaos-string
  "Generator that produces schema-violating strings (oversized for varchar limit)"
  gen-oversized-string)

(defn gen-chaos-account-insert
  "Generator for account insert with invalid data"
  []
  (gen/let [tempid gen-tempid
            chaos-name gen-chaos-string]
    {[:account/id tempid] {:account/name {:after chaos-name}}}))

(def gen-chaos-delta
  "Generator for deltas containing invalid data"
  (gen-chaos-account-insert))

;; =============================================================================
;; Chaos Mode Tests
;; =============================================================================
;; These tests verify that save-form! wraps PostgreSQL errors in ::save-error

(defspec chaos-invalid-data-throws-save-error 30
  (prop/for-all [chaos-delta gen-chaos-delta]
                (with-test-db
                  (fn [_conn env]
                    (try
                      (write/save-form! env {::rad.form/delta chaos-delta})
                      false ;; Should have thrown - test fails if we get here
                      (catch clojure.lang.ExceptionInfo e
                        (let [data (ex-data e)]
                          (and
                           ;; Must be our wrapped error type
                           (= ::write/save-error (:type data))
                           ;; Must have a known cause (oversized strings trigger 22001)
                           (= ::write/string-data-too-long (:cause data))
                           ;; Must have SQL state
                           (string? (:sql-state data))
                           ;; Must have message
                           (string? (:message data))
                           ;; Must preserve original exception
                           (instance? org.pg.error.PGErrorResponse (ex-cause e)))))
                      (catch Exception _e
                        ;; Wrong exception type - this is a bug
                        false))))))
