(ns com.fulcrologic.rad.database-adapters.sql.read-integration-test
  "Integration tests for read resolvers.

   Verifies correctness of returned data for:
   - ID resolvers (fetch entity by ID)
   - To-one resolvers (fetch related entity)
   - To-many resolvers (fetch related entities)
   - Batch queries (multiple entities at once)
   - Missing entities (nil handling)

   Uses Pathom3 lenient mode to allow nil results for missing entities."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
   [com.fulcrologic.rad.database-adapters.sql.read :as read]
   [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
   [com.fulcrologic.rad.ids :as ids]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.error :as p.error]
   [com.wsscode.pathom3.interface.eql :as p.eql]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [pg.pool :as pg.pool]
   [taoensso.encore :as enc]))

;; =============================================================================
;; Test Configuration
;; =============================================================================

(def jdbc-config
  {:jdbcUrl "jdbc:postgresql://localhost:5432/fulcro-rad-pg2?user=user&password=password"})

(def pg2-config
  {:host "localhost"
   :port 5432
   :user "user"
   :password "password"
   :database "fulcro-rad-pg2"})

(def key->attribute (enc/keys-by ::attr/qualified-key attrs/all-attributes))
(def jdbc-opts {:builder-fn rs/as-unqualified-lower-maps})

;; =============================================================================
;; Test Infrastructure
;; =============================================================================

(defn generate-test-schema-name []
  (str "test_read_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn- split-sql-statements
  "Split a multi-statement SQL string into individual statements."
  [sql]
  (->> (clojure.string/split sql #";\n")
       (map clojure.string/trim)
       (remove empty?)))

(defn with-test-db
  "Run function with isolated test database schema."
  [f]
  (let [ds (jdbc/get-datasource jdbc-config)
        schema-name (generate-test-schema-name)
        jdbc-conn (jdbc/get-connection ds)
        pg2-pool (pg.pool/pool (assoc pg2-config
                                      :pg-params {"search_path" schema-name}
                                      :pool-min-size 1
                                      :pool-max-size 2))]
    (try
      ;; Create schema and tables
      (jdbc/execute! jdbc-conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! jdbc-conn [(str "SET search_path TO " schema-name)])
      (doseq [stmt-block (mig/automatic-schema :production attrs/all-attributes)
              stmt (split-sql-statements stmt-block)]
        (jdbc/execute! jdbc-conn [stmt]))

      ;; Generate resolvers and create Pathom env with lenient mode
      ;; Lenient mode allows nil results for missing entities instead of throwing
      (let [resolvers (read/generate-resolvers attrs/all-attributes :production)
            pathom-env (-> (pci/register resolvers)
                           (assoc ::attr/key->attribute key->attribute
                                  ::rad.sql/connection-pools {:production pg2-pool}
                                  ::p.error/lenient-mode? true))]
        (f jdbc-conn pathom-env))

      (finally
        (pg.pool/close pg2-pool)
        (jdbc/execute! jdbc-conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close jdbc-conn)))))

(defn pathom-query [env query]
  (p.eql/process env query))

;; =============================================================================
;; ID Resolver Tests
;; =============================================================================

(deftest ^:integration id-resolver-returns-correct-entity
  (testing "ID resolver returns entity with all scalar fields"
    (with-test-db
      (fn [jdbc-conn env]
        (let [account-id (ids/new-uuid)]
          ;; Insert test data
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO accounts (id, name, email, active) VALUES (?, ?, ?, ?)"
                          account-id "Test Account" "test@example.com" true])

          ;; Query via Pathom
          (let [result (pathom-query env
                                     [{[:account/id account-id]
                                       [:account/id :account/name :account/email :account/active?]}])]
            (is (= account-id (get-in result [[:account/id account-id] :account/id])))
            (is (= "Test Account" (get-in result [[:account/id account-id] :account/name])))
            (is (= "test@example.com" (get-in result [[:account/id account-id] :account/email])))
            (is (= true (get-in result [[:account/id account-id] :account/active?])))))))))

(deftest ^:integration id-resolver-handles-missing-entity
  (testing "ID resolver returns nil/empty for non-existent entity"
    (with-test-db
      (fn [_jdbc-conn env]
        (let [fake-id (ids/new-uuid)
              result (pathom-query env
                                   [{[:account/id fake-id]
                                     [:account/id :account/name]}])]
          ;; Pathom returns the ident key with nil value when entity not found
          (is (nil? (get-in result [[:account/id fake-id] :account/name]))))))))

(deftest ^:integration id-resolver-handles-enum-types
  (testing "ID resolver correctly decodes enum values"
    (with-test-db
      (fn [jdbc-conn env]
        (let [addr-id (ids/new-uuid)]
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO addresses (id, street, city, state, zip) VALUES (?, ?, ?, ?, ?)"
                          addr-id "123 Main St" "Phoenix" ":state/AZ" "85001"])

          (let [result (pathom-query env
                                     [{[:address/id addr-id]
                                       [:address/id :address/street :address/city :address/state]}])]
            (is (= addr-id (get-in result [[:address/id addr-id] :address/id])))
            (is (= "123 Main St" (get-in result [[:address/id addr-id] :address/street])))
            (is (= :state/AZ (get-in result [[:address/id addr-id] :address/state])))))))))

(deftest ^:integration id-resolver-handles-keyword-and-symbol-types
  (testing "ID resolver correctly decodes keyword and symbol values"
    (with-test-db
      (fn [jdbc-conn env]
        (let [product-id (ids/new-uuid)]
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO products (id, name, quantity, sku, price, category, status) VALUES (?, ?, ?, ?, ?, ?, ?)"
                          product-id "Widget" 100 12345 19.99 ":product/electronics" "active"])

          (let [result (pathom-query env
                                     [{[:product/id product-id]
                                       [:product/id :product/name :product/category :product/status]}])]
            (is (= product-id (get-in result [[:product/id product-id] :product/id])))
            (is (= "Widget" (get-in result [[:product/id product-id] :product/name])))
            (is (= :product/electronics (get-in result [[:product/id product-id] :product/category])))
            (is (= 'active (get-in result [[:product/id product-id] :product/status])))))))))

;; =============================================================================
;; Batch Resolver Tests
;; =============================================================================

(deftest ^:integration batch-resolver-returns-all-entities
  (testing "Batch resolver returns correct data for multiple entities"
    (with-test-db
      (fn [jdbc-conn env]
        (let [ids (repeatedly 5 ids/new-uuid)]
          ;; Insert test data
          (doseq [[i id] (map-indexed vector ids)]
            (jdbc/execute! jdbc-conn
                           ["INSERT INTO accounts (id, name, active) VALUES (?, ?, ?)"
                            id (str "Account " i) true]))

          ;; Query all at once
          (let [result (pathom-query env
                                     (vec (for [id ids]
                                            {[:account/id id]
                                             [:account/id :account/name]})))]
            ;; Verify all returned correctly
            (doseq [[i id] (map-indexed vector ids)]
              (is (= id (get-in result [[:account/id id] :account/id])))
              (is (= (str "Account " i) (get-in result [[:account/id id] :account/name]))))))))))

(deftest ^:integration batch-resolver-handles-mixed-existing-and-missing
  (testing "Batch resolver returns data for existing and nil for missing"
    (with-test-db
      (fn [jdbc-conn env]
        (let [existing-id (ids/new-uuid)
              missing-id (ids/new-uuid)]
          ;; Only insert one
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO accounts (id, name) VALUES (?, ?)"
                          existing-id "Existing Account"])

          (let [result (pathom-query env
                                     [{[:account/id existing-id] [:account/id :account/name]}
                                      {[:account/id missing-id] [:account/id :account/name]}])]
            (is (= existing-id (get-in result [[:account/id existing-id] :account/id])))
            (is (= "Existing Account" (get-in result [[:account/id existing-id] :account/name])))
            (is (nil? (get-in result [[:account/id missing-id] :account/name])))))))))

;; =============================================================================
;; To-One Resolver Tests
;; =============================================================================

(deftest ^:integration to-one-forward-ref-returns-related-entity
  (testing "To-one forward ref (FK on source table) returns related entity"
    (with-test-db
      (fn [jdbc-conn env]
        (let [addr-id (ids/new-uuid)
              account-id (ids/new-uuid)]
          ;; Insert address first
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO addresses (id, street, city, state) VALUES (?, ?, ?, ?)"
                          addr-id "456 Oak Ave" "Tucson" ":state/AZ"])
          ;; Insert account with FK to address
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO accounts (id, name, primary_address) VALUES (?, ?, ?)"
                          account-id "Account With Address" addr-id])

          (let [result (pathom-query env
                                     [{[:account/id account-id]
                                       [:account/id :account/name
                                        {:account/primary-address [:address/id :address/street :address/city]}]}])]
            (is (= account-id (get-in result [[:account/id account-id] :account/id])))
            (is (= "Account With Address" (get-in result [[:account/id account-id] :account/name])))
            (is (= addr-id (get-in result [[:account/id account-id] :account/primary-address :address/id])))
            (is (= "456 Oak Ave" (get-in result [[:account/id account-id] :account/primary-address :address/street])))))))))

(deftest ^:integration to-one-forward-ref-handles-null-fk
  (testing "To-one forward ref with null FK returns nil"
    (with-test-db
      (fn [jdbc-conn env]
        (let [account-id (ids/new-uuid)]
          ;; Insert account without address
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO accounts (id, name) VALUES (?, ?)"
                          account-id "Account No Address"])

          (let [result (pathom-query env
                                     [{[:account/id account-id]
                                       [:account/id :account/name
                                        {:account/primary-address [:address/id :address/street]}]}])]
            (is (= account-id (get-in result [[:account/id account-id] :account/id])))
            (is (nil? (get-in result [[:account/id account-id] :account/primary-address])))))))))

;; =============================================================================
;; To-Many Resolver Tests
;; =============================================================================

(deftest ^:integration to-many-reverse-ref-returns-all-related-entities
  (testing "To-many reverse ref returns all related entities"
    (with-test-db
      (fn [jdbc-conn env]
        (let [account-id (ids/new-uuid)
              addr-ids (repeatedly 3 ids/new-uuid)]
          ;; Insert account
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO accounts (id, name) VALUES (?, ?)"
                          account-id "Account With Addresses"])
          ;; Insert addresses with FK to account
          (doseq [[i addr-id] (map-indexed vector addr-ids)]
            (jdbc/execute! jdbc-conn
                           ["INSERT INTO addresses (id, street, city, state, account) VALUES (?, ?, ?, ?, ?)"
                            addr-id (str "Street " i) (str "City " i) ":state/AZ" account-id]))

          (let [result (pathom-query env
                                     [{[:account/id account-id]
                                       [:account/id :account/name
                                        {:account/addresses [:address/id :address/street]}]}])
                addresses (get-in result [[:account/id account-id] :account/addresses])]
            (is (= account-id (get-in result [[:account/id account-id] :account/id])))
            (is (= 3 (count addresses)))
            (is (= (set addr-ids) (set (map :address/id addresses))))))))))

(deftest ^:integration to-many-reverse-ref-returns-empty-for-no-related
  (testing "To-many reverse ref returns empty/nil when no related entities"
    (with-test-db
      (fn [jdbc-conn env]
        (let [account-id (ids/new-uuid)]
          ;; Insert account without any addresses
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO accounts (id, name) VALUES (?, ?)"
                          account-id "Account No Addresses"])

          (let [result (pathom-query env
                                     [{[:account/id account-id]
                                       [:account/id :account/name
                                        {:account/addresses [:address/id]}]}])
                addresses (get-in result [[:account/id account-id] :account/addresses])]
            (is (= account-id (get-in result [[:account/id account-id] :account/id])))
            (is (or (nil? addresses) (empty? addresses)))))))))

;; =============================================================================
;; Self-Referential Tests
;; =============================================================================

(deftest ^:integration self-ref-to-one-parent-works
  (testing "Self-referential to-one (parent) returns correct entity"
    (with-test-db
      (fn [jdbc-conn env]
        (let [parent-id (ids/new-uuid)
              child-id (ids/new-uuid)]
          ;; Insert parent category
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO categories (id, name) VALUES (?, ?)"
                          parent-id "Parent Category"])
          ;; Insert child category with parent ref
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO categories (id, name, parent) VALUES (?, ?, ?)"
                          child-id "Child Category" parent-id])

          (let [result (pathom-query env
                                     [{[:category/id child-id]
                                       [:category/id :category/name
                                        {:category/parent [:category/id :category/name]}]}])]
            (is (= child-id (get-in result [[:category/id child-id] :category/id])))
            (is (= "Child Category" (get-in result [[:category/id child-id] :category/name])))
            (is (= parent-id (get-in result [[:category/id child-id] :category/parent :category/id])))
            (is (= "Parent Category" (get-in result [[:category/id child-id] :category/parent :category/name])))))))))

(deftest ^:integration self-ref-to-many-children-works
  (testing "Self-referential to-many (children) returns all related"
    (with-test-db
      (fn [jdbc-conn env]
        (let [parent-id (ids/new-uuid)
              child-ids (repeatedly 3 ids/new-uuid)]
          ;; Insert parent
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO categories (id, name) VALUES (?, ?)"
                          parent-id "Parent"])
          ;; Insert children
          (doseq [[i child-id] (map-indexed vector child-ids)]
            (jdbc/execute! jdbc-conn
                           ["INSERT INTO categories (id, name, parent) VALUES (?, ?, ?)"
                            child-id (str "Child " i) parent-id]))

          (let [result (pathom-query env
                                     [{[:category/id parent-id]
                                       [:category/id :category/name
                                        {:category/children [:category/id :category/name]}]}])
                children (get-in result [[:category/id parent-id] :category/children])]
            (is (= parent-id (get-in result [[:category/id parent-id] :category/id])))
            (is (= 3 (count children)))
            (is (= (set child-ids) (set (map :category/id children))))))))))

;; =============================================================================
;; Deep Nested Query Tests
;; =============================================================================

(deftest ^:integration deep-nested-query-works
  (testing "Multi-level nested query returns correct data"
    (with-test-db
      (fn [jdbc-conn env]
        (let [account-id (ids/new-uuid)
              addr-id (ids/new-uuid)]
          ;; Insert address
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO addresses (id, street, city, state) VALUES (?, ?, ?, ?)"
                          addr-id "789 Pine St" "Mesa" ":state/AZ"])
          ;; Insert account with primary address
          (jdbc/execute! jdbc-conn
                         ["INSERT INTO accounts (id, name, primary_address) VALUES (?, ?, ?)"
                          account-id "Nested Test Account" addr-id])

          ;; Query account -> primary-address -> all address fields
          (let [result (pathom-query env
                                     [{[:account/id account-id]
                                       [:account/id :account/name
                                        {:account/primary-address
                                         [:address/id :address/street :address/city :address/state :address/zip]}]}])]
            (is (= account-id (get-in result [[:account/id account-id] :account/id])))
            (is (= "Nested Test Account" (get-in result [[:account/id account-id] :account/name])))
            (is (= addr-id (get-in result [[:account/id account-id] :account/primary-address :address/id])))
            (is (= "789 Pine St" (get-in result [[:account/id account-id] :account/primary-address :address/street])))
            (is (= "Mesa" (get-in result [[:account/id account-id] :account/primary-address :address/city])))
            (is (= :state/AZ (get-in result [[:account/id account-id] :account/primary-address :address/state])))))))))

;; =============================================================================
;; Sequence-Based ID Tests
;; =============================================================================

(deftest ^:integration id-resolver-handles-sequence-based-ids
  (testing "ID resolver works with sequence-based (long) IDs"
    (with-test-db
      (fn [jdbc-conn env]
        ;; Insert item using sequence
        (let [result (jdbc/execute! jdbc-conn
                                    ["INSERT INTO items (name, quantity, active) VALUES (?, ?, ?) RETURNING id"
                                     "Test Item" 50 true]
                                    jdbc-opts)
              item-id (:id (first result))]

          ;; Query via Pathom
          (let [pathom-result (pathom-query env
                                            [{[:item/id item-id]
                                              [:item/id :item/name :item/quantity :item/active?]}])]
            (is (= item-id (get-in pathom-result [[:item/id item-id] :item/id])))
            (is (= "Test Item" (get-in pathom-result [[:item/id item-id] :item/name])))
            (is (= 50 (get-in pathom-result [[:item/id item-id] :item/quantity])))
            (is (= true (get-in pathom-result [[:item/id item-id] :item/active?])))))))))
