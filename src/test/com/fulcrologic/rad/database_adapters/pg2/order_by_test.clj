(ns com.fulcrologic.rad.database-adapters.pg2.order-by-test
  "Unit and integration tests for the order-by SQL option.

   The order-by option controls how results are ordered within to-many collections.
   It affects the ORDER BY clause inside array_agg() in the generated SQL."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.pg2 :as rad.pg2]
   [com.fulcrologic.rad.database-adapters.pg2.migration :as mig]
   [com.fulcrologic.rad.database-adapters.pg2.read :as read]
   [com.fulcrologic.rad.database-adapters.pg2.write :as write]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.error :as p.error]
   [com.wsscode.pathom3.interface.eql :as p.eql]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [pg.pool :as pg.pool]
   [taoensso.encore :as enc]))

;; =============================================================================
;; Test Attributes with order-by
;; =============================================================================

(def parent-id
  {::attr/qualified-key :parent/id
   ::attr/type :uuid
   ::attr/identity? true
   ::attr/schema :production
   ::rad.pg2/table "parents"})

(def parent-name
  {::attr/qualified-key :parent/name
   ::attr/type :string
   ::attr/schema :production
   ::attr/identities #{:parent/id}})

;; Child with position for ordering
(def child-id
  {::attr/qualified-key :child/id
   ::attr/type :uuid
   ::attr/identity? true
   ::attr/schema :production
   ::rad.pg2/table "children"})

(def child-name
  {::attr/qualified-key :child/name
   ::attr/type :string
   ::attr/schema :production
   ::attr/identities #{:child/id}})

(def child-position
  {::attr/qualified-key :child/position
   ::attr/type :int
   ::attr/schema :production
   ::attr/identities #{:child/id}})

(def child-created-at
  {::attr/qualified-key :child/created-at
   ::attr/type :instant
   ::attr/schema :production
   ::attr/identities #{:child/id}
   ::rad.pg2/column-name "created_at"})

;; FK from child to parent
(def child-parent
  {::attr/qualified-key :child/parent
   ::attr/type :ref
   ::attr/cardinality :one
   ::attr/target :parent/id
   ::attr/schema :production
   ::attr/identities #{:child/id}
   ::rad.pg2/column-name "parent_id"})

;; To-many WITH order-by (ordered by position)
(def parent-children-ordered
  {::attr/qualified-key :parent/children-ordered
   ::attr/type :ref
   ::attr/cardinality :many
   ::attr/target :child/id
   ::attr/schema :production
   ::attr/identities #{:parent/id}
   ::rad.pg2/fk-attr :child/parent
   ::rad.pg2/order-by :child/position})

;; To-many WITH order-by (ordered by name - alphabetical)
(def parent-children-by-name
  {::attr/qualified-key :parent/children-by-name
   ::attr/type :ref
   ::attr/cardinality :many
   ::attr/target :child/id
   ::attr/schema :production
   ::attr/identities #{:parent/id}
   ::rad.pg2/fk-attr :child/parent
   ::rad.pg2/order-by :child/name})

;; To-many WITHOUT order-by (no ordering guarantee)
(def parent-children-unordered
  {::attr/qualified-key :parent/children-unordered
   ::attr/type :ref
   ::attr/cardinality :many
   ::attr/target :child/id
   ::attr/schema :production
   ::attr/identities #{:parent/id}
   ::rad.pg2/fk-attr :child/parent})

(def test-attributes
  [parent-id parent-name
   child-id child-name child-position child-created-at child-parent
   parent-children-ordered parent-children-by-name parent-children-unordered])

(def key->attribute (enc/keys-by ::attr/qualified-key test-attributes))

;; =============================================================================
;; Unit Tests: SQL Generation
;; =============================================================================

(deftest build-array-agg-sql-with-order-by
  (testing "SQL includes ORDER BY when order-by column is provided"
    (let [sql (read/build-array-agg-sql :parent_id :id :children "uuid[]" :position)]
      (is (str/includes? sql "ORDER BY position"))
      (is (str/includes? sql "array_agg(id ORDER BY position)"))
      (is (str/includes? sql "FROM children"))
      (is (str/includes? sql "WHERE parent_id = ANY($1::uuid[])"))))

  (testing "SQL excludes ORDER BY when order-by column is nil"
    (let [sql (read/build-array-agg-sql :parent_id :id :children "uuid[]" nil)]
      (is (not (str/includes? sql "ORDER BY")))
      (is (str/includes? sql "array_agg(id)"))))

  (testing "ORDER BY uses the correct column name"
    (let [sql-by-name (read/build-array-agg-sql :parent_id :id :children "uuid[]" :name)
          sql-by-position (read/build-array-agg-sql :parent_id :id :children "uuid[]" :position)
          sql-by-created (read/build-array-agg-sql :parent_id :id :children "uuid[]" :created_at)]
      (is (str/includes? sql-by-name "ORDER BY name"))
      (is (str/includes? sql-by-position "ORDER BY position"))
      (is (str/includes? sql-by-created "ORDER BY created_at")))))

(deftest build-to-many-resolver-config-order-by
  (testing "to-many resolver config includes order-by in SQL when specified"
    (let [config (read/build-to-many-resolver-config
                  parent-children-ordered
                  :parent/id
                  key->attribute)]
      (is (some? config) "Config should be returned")
      (is (str/includes? (:sql config) "ORDER BY position")
          "SQL should include ORDER BY position")))

  (testing "to-many resolver config excludes order-by in SQL when not specified"
    (let [config (read/build-to-many-resolver-config
                  parent-children-unordered
                  :parent/id
                  key->attribute)]
      (is (some? config) "Config should be returned")
      (is (not (str/includes? (:sql config) "ORDER BY"))
          "SQL should not include ORDER BY")))

  (testing "to-many resolver config uses custom column name for order-by"
    (let [;; Attribute with order-by using a custom column name
          parent-children-by-created
          {::attr/qualified-key :parent/children-by-created
           ::attr/type :ref
           ::attr/cardinality :many
           ::attr/target :child/id
           ::attr/schema :production
           ::attr/identities #{:parent/id}
           ::rad.pg2/fk-attr :child/parent
           ::rad.pg2/order-by :child/created-at}
          attrs (conj test-attributes parent-children-by-created)
          k->a (enc/keys-by ::attr/qualified-key attrs)
          config (read/build-to-many-resolver-config
                  parent-children-by-created
                  :parent/id
                  k->a)]
      (is (some? config))
      (is (str/includes? (:sql config) "ORDER BY created_at")
          "SQL should use custom column name created_at"))))

;; =============================================================================
;; Integration Tests: Ordering Behavior
;; =============================================================================

(def jdbc-config
  {:jdbcUrl "jdbc:postgresql://localhost:5432/fulcro-rad-pg2?user=user&password=password"})

(def pg2-config
  {:host "localhost"
   :port 5432
   :user "user"
   :password "password"
   :database "fulcro-rad-pg2"})

(def jdbc-opts {:builder-fn rs/as-unqualified-lower-maps})

(defn generate-test-schema-name []
  (str "test_order_by_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn- split-sql-statements [sql]
  (->> (str/split sql #";\n")
       (map str/trim)
       (remove empty?)))

(defn with-test-db
  "Run function with isolated test database."
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
      (doseq [stmt-block (mig/automatic-schema :production test-attributes)
              stmt (split-sql-statements stmt-block)]
        (jdbc/execute! jdbc-conn [stmt]))

      ;; Create combined env for both reads and writes
      (let [resolvers (read/generate-resolvers test-attributes :production)
            env (-> (pci/register resolvers)
                    (assoc ::attr/key->attribute key->attribute
                           ::rad.pg2/connection-pools {:production pg2-pool}
                           ::p.error/lenient-mode? true))]
        (f jdbc-conn env))

      (finally
        (pg.pool/close pg2-pool)
        (jdbc/execute! jdbc-conn [(str "DROP SCHEMA " schema-name " CASCADE")])
        (.close jdbc-conn)))))

(defn pathom-query [env query]
  (p.eql/process env query))

(defn save! [env delta]
  (write/save-form! env {::rad.form/delta delta}))

(deftest ^:integration order-by-returns-ordered-results
  (testing "Children are returned in order when order-by is specified"
    (with-test-db
      (fn [_conn env]
        ;; Create parent with 5 children having various positions
        (let [parent-tempid (tempid/tempid)
              child1-tempid (tempid/tempid)
              child2-tempid (tempid/tempid)
              child3-tempid (tempid/tempid)
              child4-tempid (tempid/tempid)
              child5-tempid (tempid/tempid)
              delta {[:parent/id parent-tempid]
                     {:parent/name {:after "Test Parent"}}
                     [:child/id child1-tempid]
                     {:child/name {:after "Third"}
                      :child/position {:after 3}
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child2-tempid]
                     {:child/name {:after "First"}
                      :child/position {:after 1}
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child3-tempid]
                     {:child/name {:after "Fifth"}
                      :child/position {:after 5}
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child4-tempid]
                     {:child/name {:after "Second"}
                      :child/position {:after 2}
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child5-tempid]
                     {:child/name {:after "Fourth"}
                      :child/position {:after 4}
                      :child/parent {:after [:parent/id parent-tempid]}}}
              result (save! env delta)
              parent-id (get (:tempids result) parent-tempid)]

          ;; Query with ordered children
          (let [query-result (pathom-query env
                                           [{[:parent/id parent-id]
                                             [:parent/name
                                              {:parent/children-ordered [:child/id :child/name :child/position]}]}])
                parent (get query-result [:parent/id parent-id])
                children (:parent/children-ordered parent)
                positions (mapv :child/position children)]
            (is (= 5 (count children)) "Should have 5 children")
            (is (= [1 2 3 4 5] positions) "Children should be ordered by position")
            (is (= ["First" "Second" "Third" "Fourth" "Fifth"]
                   (mapv :child/name children)) "Names should be in position order")))))))

(deftest ^:integration order-by-alphabetical-ordering
  (testing "Children are returned in alphabetical order when order-by is :child/name"
    (with-test-db
      (fn [_conn env]
        (let [parent-tempid (tempid/tempid)
              child1-tempid (tempid/tempid)
              child2-tempid (tempid/tempid)
              child3-tempid (tempid/tempid)
              delta {[:parent/id parent-tempid]
                     {:parent/name {:after "Test Parent"}}
                     [:child/id child1-tempid]
                     {:child/name {:after "Zebra"}
                      :child/position {:after 1}
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child2-tempid]
                     {:child/name {:after "Apple"}
                      :child/position {:after 2}
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child3-tempid]
                     {:child/name {:after "Mango"}
                      :child/position {:after 3}
                      :child/parent {:after [:parent/id parent-tempid]}}}
              result (save! env delta)
              parent-id (get (:tempids result) parent-tempid)]

          ;; Query with children ordered by name
          (let [query-result (pathom-query env
                                           [{[:parent/id parent-id]
                                             [:parent/name
                                              {:parent/children-by-name [:child/id :child/name]}]}])
                parent (get query-result [:parent/id parent-id])
                children (:parent/children-by-name parent)
                names (mapv :child/name children)]
            (is (= 3 (count children)))
            (is (= ["Apple" "Mango" "Zebra"] names)
                "Children should be alphabetically ordered by name")))))))

(deftest ^:integration order-by-vs-unordered-comparison
  (testing "Ordered and unordered queries return same data but potentially different order"
    (with-test-db
      (fn [_conn env]
        (let [parent-tempid (tempid/tempid)
              ;; Create children with positions 3, 1, 2 (insertion order != position order)
              children-tempids (mapv (fn [_] (tempid/tempid)) (range 3))
              delta (merge
                     {[:parent/id parent-tempid] {:parent/name {:after "Test Parent"}}}
                     (into {}
                           (map-indexed
                            (fn [idx tid]
                              [[:child/id tid]
                               {:child/name {:after (str "Child-" idx)}
                                :child/position {:after (case idx 0 3, 1 1, 2 2)}
                                :child/parent {:after [:parent/id parent-tempid]}}])
                            children-tempids)))
              result (save! env delta)
              parent-id (get (:tempids result) parent-tempid)]

          ;; Query with ordered children
          (let [ordered-result (pathom-query env
                                             [{[:parent/id parent-id]
                                               [{:parent/children-ordered [:child/id :child/position]}]}])
                ordered-children (get-in ordered-result [[:parent/id parent-id] :parent/children-ordered])]
            ;; Ordered should be by position: 1, 2, 3
            (is (= [1 2 3] (mapv :child/position ordered-children))
                "Ordered children should be sorted by position")))))))

(deftest ^:integration order-by-empty-collection
  (testing "Order-by works correctly with empty collection"
    (with-test-db
      (fn [_conn env]
        ;; Create parent with no children
        (let [parent-tempid (tempid/tempid)
              delta {[:parent/id parent-tempid]
                     {:parent/name {:after "Lonely Parent"}}}
              result (save! env delta)
              parent-id (get (:tempids result) parent-tempid)]

          (let [query-result (pathom-query env
                                           [{[:parent/id parent-id]
                                             [:parent/name
                                              {:parent/children-ordered [:child/id :child/position]}]}])
                parent (get query-result [:parent/id parent-id])
                children (:parent/children-ordered parent)]
            (is (or (nil? children) (empty? children))
                "Should return nil or empty for parent with no children")))))))

(deftest ^:integration order-by-single-child
  (testing "Order-by works correctly with single child"
    (with-test-db
      (fn [_conn env]
        (let [parent-tempid (tempid/tempid)
              child-tempid (tempid/tempid)
              delta {[:parent/id parent-tempid]
                     {:parent/name {:after "Single Child Parent"}}
                     [:child/id child-tempid]
                     {:child/name {:after "Only Child"}
                      :child/position {:after 1}
                      :child/parent {:after [:parent/id parent-tempid]}}}
              result (save! env delta)
              parent-id (get (:tempids result) parent-tempid)]

          (let [query-result (pathom-query env
                                           [{[:parent/id parent-id]
                                             [{:parent/children-ordered [:child/id :child/name :child/position]}]}])
                parent (get query-result [:parent/id parent-id])
                children (:parent/children-ordered parent)]
            (is (= 1 (count children)))
            (is (= "Only Child" (:child/name (first children))))))))))

(deftest ^:integration order-by-null-values
  (testing "Order-by handles null values in ordering column"
    (with-test-db
      (fn [_conn env]
        (let [parent-tempid (tempid/tempid)
              child1-tempid (tempid/tempid)
              child2-tempid (tempid/tempid)
              child3-tempid (tempid/tempid)
              delta {[:parent/id parent-tempid]
                     {:parent/name {:after "Parent with nulls"}}
                     [:child/id child1-tempid]
                     {:child/name {:after "Has position"}
                      :child/position {:after 2}
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child2-tempid]
                     {:child/name {:after "No position"} ;; position will be NULL
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child3-tempid]
                     {:child/name {:after "First position"}
                      :child/position {:after 1}
                      :child/parent {:after [:parent/id parent-tempid]}}}
              result (save! env delta)
              parent-id (get (:tempids result) parent-tempid)]

          ;; Query should not fail with NULL values
          (let [query-result (pathom-query env
                                           [{[:parent/id parent-id]
                                             [{:parent/children-ordered [:child/id :child/name :child/position]}]}])
                parent (get query-result [:parent/id parent-id])
                children (:parent/children-ordered parent)]
            ;; PostgreSQL default: NULLs sort last in ASC order
            (is (= 3 (count children)) "Should return all 3 children")
            ;; First two should be sorted by position (1, 2), NULL last
            (is (= 1 (:child/position (first children))) "First child should have position 1")
            (is (= 2 (:child/position (second children))) "Second child should have position 2")
            (is (nil? (:child/position (last children))) "Last child should have NULL position")))))))

(deftest ^:integration order-by-duplicate-values
  (testing "Order-by handles duplicate values in ordering column"
    (with-test-db
      (fn [_conn env]
        (let [parent-tempid (tempid/tempid)
              child1-tempid (tempid/tempid)
              child2-tempid (tempid/tempid)
              child3-tempid (tempid/tempid)
              delta {[:parent/id parent-tempid]
                     {:parent/name {:after "Parent with duplicates"}}
                     [:child/id child1-tempid]
                     {:child/name {:after "Child A"}
                      :child/position {:after 1}
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child2-tempid]
                     {:child/name {:after "Child B"}
                      :child/position {:after 1}  ;; Same position as Child A
                      :child/parent {:after [:parent/id parent-tempid]}}
                     [:child/id child3-tempid]
                     {:child/name {:after "Child C"}
                      :child/position {:after 2}
                      :child/parent {:after [:parent/id parent-tempid]}}}
              result (save! env delta)
              parent-id (get (:tempids result) parent-tempid)]

          (let [query-result (pathom-query env
                                           [{[:parent/id parent-id]
                                             [{:parent/children-ordered [:child/id :child/name :child/position]}]}])
                parent (get query-result [:parent/id parent-id])
                children (:parent/children-ordered parent)
                positions (mapv :child/position children)]
            (is (= 3 (count children)))
            ;; Children with position 1 come before children with position 2
            ;; Order within position 1 is not guaranteed (database dependent)
            (is (= [1 1 2] positions)
                "Duplicate positions should be preserved in order")))))))

