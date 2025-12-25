(ns com.fulcrologic.rad.database-adapters.pg2.migration-test
  "Tests for migration.clj - SQL type mapping, schema generation, and migration parsing."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.attributes-options :as ao]
   [com.fulcrologic.rad.database-adapters.pg2 :as rad.pg2]
   [com.fulcrologic.rad.database-adapters.pg2.migration :as mig]
   [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
   [taoensso.encore :as enc]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def key->attribute (enc/keys-by ::attr/qualified-key attrs/all-attributes))

;; =============================================================================
;; type-map tests
;; =============================================================================

(deftest type-map-contains-all-standard-types
  (testing "type-map has entries for all standard RAD types"
    (is (= "VARCHAR(2048)" (get mig/type-map :string)))
    (is (= "VARCHAR(512)" (get mig/type-map :password)))
    (is (= "BOOLEAN" (get mig/type-map :boolean)))
    (is (= "INTEGER" (get mig/type-map :int)))
    (is (= "BIGINT" (get mig/type-map :long)))
    (is (= "decimal(20,2)" (get mig/type-map :decimal)))
    (is (= "TIMESTAMP WITH TIME ZONE" (get mig/type-map :instant)))
    (is (= "VARCHAR(200)" (get mig/type-map :enum)))
    (is (= "VARCHAR(200)" (get mig/type-map :keyword)))
    (is (= "VARCHAR(200)" (get mig/type-map :symbol)))
    (is (= "UUID" (get mig/type-map :uuid)))))

;; =============================================================================
;; parse-migration-version tests
;; =============================================================================

(deftest parse-migration-version-test
  (testing "extracts version from standard migration filenames"
    (is (= "V001" (#'mig/parse-migration-version "V001__create_users.sql")))
    (is (= "V002" (#'mig/parse-migration-version "V002__add_email_column.sql")))
    (is (= "V123" (#'mig/parse-migration-version "V123__complex_migration_name.sql"))))

  (testing "handles multi-digit versions"
    (is (= "V0001" (#'mig/parse-migration-version "V0001__init.sql")))
    (is (= "V99999" (#'mig/parse-migration-version "V99999__final.sql"))))

  (testing "returns nil for invalid filenames"
    (is (nil? (#'mig/parse-migration-version "not_a_migration.sql")))
    (is (nil? (#'mig/parse-migration-version "V__missing_number.sql")))
    (is (nil? (#'mig/parse-migration-version "V001_missing_underscore.sql")))
    (is (nil? (#'mig/parse-migration-version "V001__no_extension")))
    (is (nil? (#'mig/parse-migration-version "readme.md")))
    (is (nil? (#'mig/parse-migration-version ""))))

  (testing "requires leading V"
    (is (nil? (#'mig/parse-migration-version "001__create_users.sql")))
    (is (nil? (#'mig/parse-migration-version "v001__lowercase.sql")))))

;; =============================================================================
;; Schema operation constructor tests
;; =============================================================================

(deftest new-table-test
  (testing "creates table operation"
    (let [result (mig/new-table "users")]
      (is (= :table (:type result)))
      (is (= "users" (:table result))))))

(deftest new-scalar-column-test
  (testing "creates scalar column operation"
    (let [attr {::attr/qualified-key :user/name ::attr/type :string}
          result (mig/new-scalar-column "users" "name" attr)]
      (is (= :column (:type result)))
      (is (= "users" (:table result)))
      (is (= "name" (:column result)))
      (is (= attr (:attr result))))))

(deftest new-id-column-test
  (testing "creates id column operation"
    (let [attr {::attr/qualified-key :user/id ::attr/type :uuid ::attr/identity? true}
          result (mig/new-id-column "users" "id" attr)]
      (is (= :id (:type result)))
      (is (= "users" (:table result)))
      (is (= "id" (:column result)))
      (is (= attr (:attr result))))))

(deftest new-ref-column-test
  (testing "creates ref column operation"
    (let [attr {::attr/qualified-key :order/customer
                ::attr/type :ref
                ::attr/target :customer/id}
          result (mig/new-ref-column "orders" "customer_id" attr)]
      (is (= :ref (:type result)))
      (is (= "orders" (:table result)))
      (is (= "customer_id" (:column result)))
      (is (= attr (:attr result))))))

;; =============================================================================
;; op->sql tests
;; =============================================================================

(deftest op->sql-table-test
  (testing "generates CREATE TABLE statement"
    (let [op {:type :table :table "users"}
          result (mig/op->sql {} nil op)]
      (is (= "CREATE TABLE IF NOT EXISTS users ();\n" result)))))

(deftest op->sql-column-test
  (testing "generates ALTER TABLE ADD COLUMN for string type"
    (let [attr {::attr/qualified-key :user/name ::attr/type :string}
          op {:type :column :table "users" :column "name" :attr attr}
          result (mig/op->sql {} nil op)]
      (is (= "ALTER TABLE users ADD COLUMN IF NOT EXISTS name VARCHAR(200);\n" result))))

  (testing "generates ALTER TABLE ADD COLUMN for int type"
    (let [attr {::attr/qualified-key :user/age ::attr/type :int}
          op {:type :column :table "users" :column "age" :attr attr}
          result (mig/op->sql {} nil op)]
      (is (= "ALTER TABLE users ADD COLUMN IF NOT EXISTS age INTEGER;\n" result))))

  (testing "generates ALTER TABLE ADD COLUMN for boolean type"
    (let [attr {::attr/qualified-key :user/active ::attr/type :boolean}
          op {:type :column :table "users" :column "active" :attr attr}
          result (mig/op->sql {} nil op)]
      (is (= "ALTER TABLE users ADD COLUMN IF NOT EXISTS active BOOLEAN;\n" result))))

  (testing "generates ALTER TABLE ADD COLUMN for uuid type"
    (let [attr {::attr/qualified-key :user/token ::attr/type :uuid}
          op {:type :column :table "users" :column "token" :attr attr}
          result (mig/op->sql {} nil op)]
      (is (= "ALTER TABLE users ADD COLUMN IF NOT EXISTS token UUID;\n" result))))

  (testing "respects max-length for string types"
    (let [attr {::attr/qualified-key :user/bio ::attr/type :string ::rad.pg2/max-length 1000}
          op {:type :column :table "users" :column "bio" :attr attr}
          result (mig/op->sql {} nil op)]
      (is (= "ALTER TABLE users ADD COLUMN IF NOT EXISTS bio VARCHAR(1000);\n" result)))))

(deftest op->sql-id-uuid-test
  (testing "generates id column for UUID type"
    (let [attr {::attr/qualified-key :user/id ::attr/type :uuid ::attr/identity? true
                ::rad.pg2/table "users"}
          op {:type :id :table "users" :column "id" :attr attr}
          result (mig/op->sql {} nil op)]
      (is (str/includes? result "ALTER TABLE users ADD COLUMN IF NOT EXISTS id UUID"))
      (is (str/includes? result "CREATE UNIQUE INDEX IF NOT EXISTS users_id_idx ON users(id)")))))

(deftest op->sql-id-sequence-test
  (testing "generates id column with sequence for int type"
    (let [attr {::attr/qualified-key :item/id ::attr/type :int ::attr/identity? true
                ::rad.pg2/table "items"}
          op {:type :id :table "items" :column "id" :attr attr}
          result (mig/op->sql {} nil op)]
      (is (str/includes? result "CREATE SEQUENCE IF NOT EXISTS items_id_seq"))
      (is (str/includes? result "ALTER TABLE items ADD COLUMN IF NOT EXISTS id INTEGER DEFAULT nextval('items_id_seq')"))
      (is (str/includes? result "CREATE UNIQUE INDEX IF NOT EXISTS items_id_idx ON items(id)"))))

  (testing "generates id column with sequence for long type"
    (let [attr {::attr/qualified-key :event/id ::attr/type :long ::attr/identity? true
                ::rad.pg2/table "events"}
          op {:type :id :table "events" :column "id" :attr attr}
          result (mig/op->sql {} nil op)]
      (is (str/includes? result "CREATE SEQUENCE IF NOT EXISTS events_id_seq"))
      (is (str/includes? result "BIGINT DEFAULT nextval('events_id_seq')")))))

;; =============================================================================
;; automatic-schema tests
;; =============================================================================

(deftest automatic-schema-generates-sql
  (testing "generates SQL for production schema attributes"
    (let [result (mig/automatic-schema :production attrs/all-attributes)]
      ;; Should return a vector of SQL strings
      (is (vector? result))
      (is (every? string? result))
      ;; Should contain table creation statements
      (is (some #(str/includes? % "CREATE TABLE") result))
      ;; Should contain column statements
      (is (some #(str/includes? % "ADD COLUMN") result)))))

(deftest automatic-schema-creates-expected-tables
  (testing "creates accounts table"
    (let [result (mig/automatic-schema :production attrs/all-attributes)
          all-sql (str/join "\n" result)]
      (is (str/includes? all-sql "CREATE TABLE IF NOT EXISTS accounts"))))

  (testing "creates addresses table"
    (let [result (mig/automatic-schema :production attrs/all-attributes)
          all-sql (str/join "\n" result)]
      (is (str/includes? all-sql "CREATE TABLE IF NOT EXISTS addresses"))))

  (testing "creates products table"
    (let [result (mig/automatic-schema :production attrs/all-attributes)
          all-sql (str/join "\n" result)]
      (is (str/includes? all-sql "CREATE TABLE IF NOT EXISTS products")))))

(deftest automatic-schema-creates-indexes
  (testing "creates indexes for id columns"
    (let [result (mig/automatic-schema :production attrs/all-attributes)
          all-sql (str/join "\n" result)]
      (is (str/includes? all-sql "CREATE UNIQUE INDEX")))))

(deftest automatic-schema-handles-empty-schema
  (testing "returns empty vector for non-existent schema"
    (let [result (mig/automatic-schema :nonexistent attrs/all-attributes)]
      (is (vector? result))
      (is (empty? result)))))

(deftest automatic-schema-respects-max-length
  (testing "uses max-length in generated column definitions"
    ;; account-name has max-length 200
    (let [result (mig/automatic-schema :production attrs/all-attributes)
          all-sql (str/join "\n" result)]
      ;; The name column should use VARCHAR(200) due to max-length
      (is (str/includes? all-sql "VARCHAR(200)")))))

;; =============================================================================
;; attr->ops tests
;; =============================================================================

(deftest attr->ops-returns-nil-for-wrong-schema
  (testing "returns nil for attributes not in target schema"
    (let [attr {::attr/qualified-key :other/name
                ::attr/type :string
                ::attr/schema :other-schema
                ::attr/identities #{:other/id}}
          result (mig/attr->ops :production key->attribute attr)]
      (is (nil? result)))))

(deftest attr->ops-creates-table-and-column-for-scalar
  (testing "creates table and column ops for scalar attribute"
    (let [result (mig/attr->ops :production key->attribute attrs/account-name)]
      (is (vector? result))
      (is (= 2 (count result)))
      (is (= :table (:type (first result))))
      (is (= :column (:type (second result)))))))

(deftest attr->ops-creates-table-and-id-for-identity
  (testing "creates table and id ops for identity attribute"
    (let [result (mig/attr->ops :production key->attribute attrs/account-id)]
      (is (vector? result))
      (is (= 2 (count result)))
      (is (= :table (:type (first result))))
      (is (= :id (:type (second result)))))))
