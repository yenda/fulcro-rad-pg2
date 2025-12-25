(ns com.fulcrologic.rad.database-adapters.pg2.ref-lifecycle-test
  "Integration tests for reference lifecycle with Pathom queries.

   Tests the full lifecycle of references including:
   - Creating entities with refs
   - Reading via Pathom queries
   - Modifying refs (change, remove)
   - delete-orphan? behavior
   - Verifying state via queries after mutations"
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.pg2 :as rad.pg2]
   [com.fulcrologic.rad.database-adapters.pg2.migration :as mig]
   [com.fulcrologic.rad.database-adapters.pg2.read :as read]
   [com.fulcrologic.rad.database-adapters.pg2.write :as write]
   [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
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
  (str "test_lifecycle_" (System/currentTimeMillis) "_" (rand-int 10000)))

(defn- split-sql-statements [sql]
  (->> (str/split sql #";\n")
       (map str/trim)
       (remove empty?)))

(defn with-test-db
  "Run function with isolated test database, both write and read capabilities."
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

      ;; Create combined env for both reads and writes
      (let [resolvers (read/generate-resolvers attrs/all-attributes :production)
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

;; =============================================================================
;; Helper: Count entities
;; =============================================================================

(defn count-rows [conn table]
  (:count (first (jdbc/execute! conn [(str "SELECT COUNT(*) as count FROM " table)] jdbc-opts))))

;; =============================================================================
;; Document/Metadata Lifecycle Tests
;;
;; Pattern: document/metadata has `fk-attr :metadata/document` and `delete-orphan?`
;; The FK is on metadata (metadata.document_id)
;; =============================================================================

(deftest ^:integration create-document-with-metadata-and-query
  (testing "Create document with metadata, verify via Pathom query"
    (with-test-db
      (fn [_conn env]
        (let [doc-tempid (tempid/tempid)
              meta-tempid (tempid/tempid)
              delta {[:document/id doc-tempid]
                     {:document/title {:after "My Document"}
                      :document/metadata {:after [:metadata/id meta-tempid]}}
                     [:metadata/id meta-tempid]
                     {:metadata/author {:after "John Doe"}
                      :metadata/version {:after 1}}}
              result (save! env delta)
              doc-id (get (:tempids result) doc-tempid)
              meta-id (get (:tempids result) meta-tempid)]

          ;; Query document with metadata
          (let [query-result (pathom-query env
                                           [{[:document/id doc-id]
                                             [:document/id :document/title
                                              {:document/metadata [:metadata/id :metadata/author :metadata/version]}]}])
                doc (get query-result [:document/id doc-id])]
            (is (= "My Document" (:document/title doc)))
            (is (= meta-id (get-in doc [:document/metadata :metadata/id])))
            (is (= "John Doe" (get-in doc [:document/metadata :metadata/author])))
            (is (= 1 (get-in doc [:document/metadata :metadata/version])))))))))

(deftest ^:integration change-metadata-deletes-old-verified-by-query
  (testing "Changing document's metadata deletes old, verified via Pathom"
    (with-test-db
      (fn [conn env]
        ;; Setup: create document with metadata
        (let [doc-tempid (tempid/tempid)
              old-meta-tempid (tempid/tempid)
              setup-delta {[:document/id doc-tempid]
                           {:document/title {:after "Doc"}
                            :document/metadata {:after [:metadata/id old-meta-tempid]}}
                           [:metadata/id old-meta-tempid]
                           {:metadata/author {:after "Old Author"}
                            :metadata/version {:after 1}}}
              setup-result (save! env setup-delta)
              doc-id (get (:tempids setup-result) doc-tempid)
              old-meta-id (get (:tempids setup-result) old-meta-tempid)]

          ;; Verify initial state
          (is (= 1 (count-rows conn "metadata")))

          ;; Create new metadata and change reference
          (let [new-meta-tempid (tempid/tempid)
                change-delta {[:document/id doc-id]
                              {:document/metadata {:before [:metadata/id old-meta-id]
                                                   :after [:metadata/id new-meta-tempid]}}
                              [:metadata/id new-meta-tempid]
                              {:metadata/author {:after "New Author"}
                               :metadata/version {:after 2}}}
                change-result (save! env change-delta)
                new-meta-id (get (:tempids change-result) new-meta-tempid)]

            ;; Old metadata should be deleted (delete-orphan? true)
            (is (= 1 (count-rows conn "metadata")) "Should still have 1 metadata (old deleted, new created)")

            ;; Query to verify new metadata is linked
            (let [query-result (pathom-query env
                                             [{[:document/id doc-id]
                                               [:document/id
                                                {:document/metadata [:metadata/id :metadata/author]}]}])
                  doc (get query-result [:document/id doc-id])]
              (is (= new-meta-id (get-in doc [:document/metadata :metadata/id])))
              (is (= "New Author" (get-in doc [:document/metadata :metadata/author]))))

            ;; Query old metadata directly - should not exist
            (let [old-query (pathom-query env
                                          [{[:metadata/id old-meta-id]
                                            [:metadata/id :metadata/author]}])
                  old-meta (get old-query [:metadata/id old-meta-id])]
              (is (nil? (:metadata/author old-meta)) "Old metadata should not exist"))))))))

(deftest ^:integration remove-metadata-deletes-orphan-verified-by-query
  (testing "Removing document's metadata deletes the orphan, verified via Pathom"
    (with-test-db
      (fn [conn env]
        ;; Setup: create document with metadata
        (let [doc-tempid (tempid/tempid)
              meta-tempid (tempid/tempid)
              setup-delta {[:document/id doc-tempid]
                           {:document/title {:after "Doc"}
                            :document/metadata {:after [:metadata/id meta-tempid]}}
                           [:metadata/id meta-tempid]
                           {:metadata/author {:after "Soon Orphan"}
                            :metadata/version {:after 1}}}
              setup-result (save! env setup-delta)
              doc-id (get (:tempids setup-result) doc-tempid)
              meta-id (get (:tempids setup-result) meta-tempid)]

          ;; Verify initial state
          (is (= 1 (count-rows conn "metadata")))
          (is (= 1 (count-rows conn "documents")))

          ;; Remove reference
          (let [remove-delta {[:document/id doc-id]
                              {:document/metadata {:before [:metadata/id meta-id]
                                                   :after nil}}}]
            (save! env remove-delta)

            ;; Metadata should be deleted
            (is (= 0 (count-rows conn "metadata")) "Orphaned metadata should be deleted")
            (is (= 1 (count-rows conn "documents")) "Document should still exist")

            ;; Query document - should have no metadata
            (let [query-result (pathom-query env
                                             [{[:document/id doc-id]
                                               [:document/id :document/title
                                                {:document/metadata [:metadata/id]}]}])
                  doc (get query-result [:document/id doc-id])]
              (is (= "Doc" (:document/title doc)))
              (is (nil? (:document/metadata doc)) "Document should have no metadata"))

            ;; Query metadata directly - should not exist
            (let [meta-query (pathom-query env
                                           [{[:metadata/id meta-id]
                                             [:metadata/id :metadata/author]}])
                  meta (get meta-query [:metadata/id meta-id])]
              (is (nil? (:metadata/author meta)) "Metadata should not exist"))))))))

;; =============================================================================
;; Account/Addresses (to-many) Lifecycle Tests
;; =============================================================================

(deftest ^:integration to-many-add-and-remove-with-query
  (testing "Add and remove from to-many collection, verify via Pathom"
    (with-test-db
      (fn [conn env]
        ;; Setup: create account with two addresses
        (let [acct-tempid (tempid/tempid)
              addr1-tempid (tempid/tempid)
              addr2-tempid (tempid/tempid)
              setup-delta {[:account/id acct-tempid]
                           {:account/name {:after "Test Account"}
                            :account/addresses {:after [[:address/id addr1-tempid]
                                                        [:address/id addr2-tempid]]}}
                           [:address/id addr1-tempid]
                           {:address/street {:after "123 First St"}}
                           [:address/id addr2-tempid]
                           {:address/street {:after "456 Second St"}}}
              setup-result (save! env setup-delta)
              acct-id (get (:tempids setup-result) acct-tempid)
              addr1-id (get (:tempids setup-result) addr1-tempid)
              addr2-id (get (:tempids setup-result) addr2-tempid)]

          ;; Verify initial state
          (is (= 2 (count-rows conn "addresses")))

          ;; Query account with addresses
          (let [query-result (pathom-query env
                                           [{[:account/id acct-id]
                                             [:account/id :account/name
                                              {:account/addresses [:address/id :address/street]}]}])
                acct (get query-result [:account/id acct-id])
                addresses (:account/addresses acct)]
            (is (= 2 (count addresses)))
            (is (some #(= "123 First St" (:address/street %)) addresses))
            (is (some #(= "456 Second St" (:address/street %)) addresses)))

          ;; Note: account/addresses doesn't have delete-orphan?, so removing
          ;; an address from the collection just clears the FK, doesn't delete
          ;; Let's test that by removing addr1 from the collection
          (let [remove-delta {[:account/id acct-id]
                              {:account/addresses {:before [[:address/id addr1-id]
                                                            [:address/id addr2-id]]
                                                   :after [[:address/id addr2-id]]}}}]
            (save! env remove-delta)

            ;; Both addresses should still exist (no delete-orphan?)
            (is (= 2 (count-rows conn "addresses")) "Addresses should still exist (no delete-orphan?)")

            ;; But account should only show one address
            (let [query-result (pathom-query env
                                             [{[:account/id acct-id]
                                               [:account/id
                                                {:account/addresses [:address/id :address/street]}]}])
                  acct (get query-result [:account/id acct-id])
                  addresses (:account/addresses acct)]
              (is (= 1 (count addresses)))
              (is (= "456 Second St" (:address/street (first addresses)))))))))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest ^:integration create-child-then-link-to-parent
  (testing "Create metadata first, then link it to a document"
    (with-test-db
      (fn [_conn env]
        ;; Create orphan metadata first
        (let [meta-tempid (tempid/tempid)
              meta-delta {[:metadata/id meta-tempid]
                          {:metadata/author {:after "Orphan Author"}
                           :metadata/version {:after 1}}}
              meta-result (save! env meta-delta)
              meta-id (get (:tempids meta-result) meta-tempid)]

          ;; Query metadata - should exist but have no document
          (let [query-result (pathom-query env
                                           [{[:metadata/id meta-id]
                                             [:metadata/id :metadata/author
                                              {:metadata/document [:document/id]}]}])
                meta (get query-result [:metadata/id meta-id])]
            (is (= "Orphan Author" (:metadata/author meta)))
            (is (nil? (:metadata/document meta))))

          ;; Now create document and link to existing metadata
          (let [doc-tempid (tempid/tempid)
                doc-delta {[:document/id doc-tempid]
                           {:document/title {:after "New Doc"}
                            :document/metadata {:after [:metadata/id meta-id]}}}
                doc-result (save! env doc-delta)
                doc-id (get (:tempids doc-result) doc-tempid)]

            ;; Query document with metadata
            (let [query-result (pathom-query env
                                             [{[:document/id doc-id]
                                               [:document/id :document/title
                                                {:document/metadata [:metadata/id :metadata/author]}]}])
                  doc (get query-result [:document/id doc-id])]
              (is (= "New Doc" (:document/title doc)))
              (is (= meta-id (get-in doc [:document/metadata :metadata/id])))
              (is (= "Orphan Author" (get-in doc [:document/metadata :metadata/author]))))))))))

(deftest ^:integration multiple-documents-same-session
  (testing "Create multiple documents with metadata in same session"
    (with-test-db
      (fn [conn env]
        (let [doc1-tempid (tempid/tempid)
              meta1-tempid (tempid/tempid)
              doc2-tempid (tempid/tempid)
              meta2-tempid (tempid/tempid)
              delta {[:document/id doc1-tempid]
                     {:document/title {:after "Doc 1"}
                      :document/metadata {:after [:metadata/id meta1-tempid]}}
                     [:metadata/id meta1-tempid]
                     {:metadata/author {:after "Author 1"}}
                     [:document/id doc2-tempid]
                     {:document/title {:after "Doc 2"}
                      :document/metadata {:after [:metadata/id meta2-tempid]}}
                     [:metadata/id meta2-tempid]
                     {:metadata/author {:after "Author 2"}}}
              result (save! env delta)
              doc1-id (get (:tempids result) doc1-tempid)
              doc2-id (get (:tempids result) doc2-tempid)]

          (is (= 2 (count-rows conn "documents")))
          (is (= 2 (count-rows conn "metadata")))

          ;; Query both documents
          (let [query-result (pathom-query env
                                           [{[:document/id doc1-id]
                                             [:document/id :document/title
                                              {:document/metadata [:metadata/author]}]}
                                            {[:document/id doc2-id]
                                             [:document/id :document/title
                                              {:document/metadata [:metadata/author]}]}])
                doc1 (get query-result [:document/id doc1-id])
                doc2 (get query-result [:document/id doc2-id])]
            (is (= "Doc 1" (:document/title doc1)))
            (is (= "Author 1" (get-in doc1 [:document/metadata :metadata/author])))
            (is (= "Doc 2" (:document/title doc2)))
            (is (= "Author 2" (get-in doc2 [:document/metadata :metadata/author])))))))))

;; =============================================================================
;; Bidirectional FK Tests (Self-Referential)
;;
;; Pattern: category/parent (to-one, stores FK directly)
;;          category/children (to-many, reverse lookup via `ref :category/parent`)
;; =============================================================================

(deftest ^:integration self-ref-create-parent-child-hierarchy
  (testing "Create parent and children via save!, query both directions"
    (with-test-db
      (fn [conn env]
        ;; Create parent and two children in one operation
        (let [parent-tempid (tempid/tempid)
              child1-tempid (tempid/tempid)
              child2-tempid (tempid/tempid)
              delta {[:category/id parent-tempid]
                     {:category/name {:after "Electronics"}}
                     [:category/id child1-tempid]
                     {:category/name {:after "Phones"}
                      :category/parent {:after [:category/id parent-tempid]}}
                     [:category/id child2-tempid]
                     {:category/name {:after "Tablets"}
                      :category/parent {:after [:category/id parent-tempid]}}}
              result (save! env delta)
              parent-id (get (:tempids result) parent-tempid)
              child1-id (get (:tempids result) child1-tempid)
              child2-id (get (:tempids result) child2-tempid)]

          (is (= 3 (count-rows conn "categories")))

          ;; Query child -> parent (forward direction, FK on child)
          (let [query-result (pathom-query env
                                           [{[:category/id child1-id]
                                             [:category/id :category/name
                                              {:category/parent [:category/id :category/name]}]}])
                child (get query-result [:category/id child1-id])]
            (is (= "Phones" (:category/name child)))
            (is (= parent-id (get-in child [:category/parent :category/id])))
            (is (= "Electronics" (get-in child [:category/parent :category/name]))))

          ;; Query parent -> children (reverse direction via `ref`)
          (let [query-result (pathom-query env
                                           [{[:category/id parent-id]
                                             [:category/id :category/name
                                              {:category/children [:category/id :category/name]}]}])
                parent (get query-result [:category/id parent-id])
                children (:category/children parent)]
            (is (= "Electronics" (:category/name parent)))
            (is (= 2 (count children)))
            (is (= #{child1-id child2-id} (set (map :category/id children))))
            (is (= #{"Phones" "Tablets"} (set (map :category/name children))))))))))

(deftest ^:integration self-ref-change-parent
  (testing "Change a child's parent, verify both old and new parent's children lists"
    (with-test-db
      (fn [conn env]
        ;; Create two parents and one child
        (let [parent1-tempid (tempid/tempid)
              parent2-tempid (tempid/tempid)
              child-tempid (tempid/tempid)
              setup-delta {[:category/id parent1-tempid]
                           {:category/name {:after "Electronics"}}
                           [:category/id parent2-tempid]
                           {:category/name {:after "Appliances"}}
                           [:category/id child-tempid]
                           {:category/name {:after "Smart TV"}
                            :category/parent {:after [:category/id parent1-tempid]}}}
              setup-result (save! env setup-delta)
              parent1-id (get (:tempids setup-result) parent1-tempid)
              parent2-id (get (:tempids setup-result) parent2-tempid)
              child-id (get (:tempids setup-result) child-tempid)]

          (is (= 3 (count-rows conn "categories")))

          ;; Verify initial state - child belongs to parent1
          (let [query-result (pathom-query env
                                           [{[:category/id parent1-id]
                                             [:category/id {:category/children [:category/id]}]}
                                            {[:category/id parent2-id]
                                             [:category/id {:category/children [:category/id]}]}])
                parent1-children (get-in query-result [[:category/id parent1-id] :category/children])
                parent2-children (get-in query-result [[:category/id parent2-id] :category/children])]
            (is (= [child-id] (mapv :category/id parent1-children)))
            (is (empty? parent2-children)))

          ;; Change child's parent from parent1 to parent2
          (let [change-delta {[:category/id child-id]
                              {:category/parent {:before [:category/id parent1-id]
                                                 :after [:category/id parent2-id]}}}]
            (save! env change-delta)

            ;; Query to verify parent change
            (let [query-result (pathom-query env
                                             [{[:category/id child-id]
                                               [:category/id :category/name
                                                {:category/parent [:category/id :category/name]}]}])
                  child (get query-result [:category/id child-id])]
              (is (= parent2-id (get-in child [:category/parent :category/id])))
              (is (= "Appliances" (get-in child [:category/parent :category/name]))))

            ;; Query both parents' children lists
            (let [query-result (pathom-query env
                                             [{[:category/id parent1-id]
                                               [:category/id {:category/children [:category/id]}]}
                                              {[:category/id parent2-id]
                                               [:category/id {:category/children [:category/id]}]}])
                  parent1-children (get-in query-result [[:category/id parent1-id] :category/children])
                  parent2-children (get-in query-result [[:category/id parent2-id] :category/children])]
              (is (empty? parent1-children) "Child should no longer be under parent1")
              (is (= [child-id] (mapv :category/id parent2-children)) "Child should now be under parent2"))))))))

(deftest ^:integration self-ref-remove-parent-orphans-child
  (testing "Remove parent reference, child becomes orphan (no delete-orphan? on parent attr)"
    (with-test-db
      (fn [conn env]
        ;; Create parent and child
        (let [parent-tempid (tempid/tempid)
              child-tempid (tempid/tempid)
              setup-delta {[:category/id parent-tempid]
                           {:category/name {:after "Electronics"}}
                           [:category/id child-tempid]
                           {:category/name {:after "Phones"}
                            :category/parent {:after [:category/id parent-tempid]}}}
              setup-result (save! env setup-delta)
              parent-id (get (:tempids setup-result) parent-tempid)
              child-id (get (:tempids setup-result) child-tempid)]

          (is (= 2 (count-rows conn "categories")))

          ;; Remove parent reference from child (set to nil)
          (let [remove-delta {[:category/id child-id]
                              {:category/parent {:before [:category/id parent-id]
                                                 :after nil}}}]
            (save! env remove-delta)

            ;; Both categories should still exist (no delete-orphan? on category/parent)
            (is (= 2 (count-rows conn "categories")) "Both categories should still exist")

            ;; Child should have no parent
            (let [query-result (pathom-query env
                                             [{[:category/id child-id]
                                               [:category/id :category/name
                                                {:category/parent [:category/id]}]}])
                  child (get query-result [:category/id child-id])]
              (is (= "Phones" (:category/name child)))
              (is (nil? (:category/parent child)) "Child should have no parent"))

            ;; Parent should have no children
            (let [query-result (pathom-query env
                                             [{[:category/id parent-id]
                                               [:category/id {:category/children [:category/id]}]}])
                  children (get-in query-result [[:category/id parent-id] :category/children])]
              (is (empty? children) "Parent should have no children"))))))))

(deftest ^:integration self-ref-nested-hierarchy
  (testing "Create and query 3-level hierarchy: grandparent -> parent -> child"
    (with-test-db
      (fn [conn env]
        (let [gp-tempid (tempid/tempid)
              p-tempid (tempid/tempid)
              c-tempid (tempid/tempid)
              delta {[:category/id gp-tempid]
                     {:category/name {:after "All Products"}}
                     [:category/id p-tempid]
                     {:category/name {:after "Electronics"}
                      :category/parent {:after [:category/id gp-tempid]}}
                     [:category/id c-tempid]
                     {:category/name {:after "Phones"}
                      :category/parent {:after [:category/id p-tempid]}}}
              result (save! env delta)
              gp-id (get (:tempids result) gp-tempid)
              p-id (get (:tempids result) p-tempid)
              c-id (get (:tempids result) c-tempid)]

          (is (= 3 (count-rows conn "categories")))

          ;; Query from child up through all ancestors
          (let [query-result (pathom-query env
                                           [{[:category/id c-id]
                                             [:category/id :category/name
                                              {:category/parent
                                               [:category/id :category/name
                                                {:category/parent
                                                 [:category/id :category/name]}]}]}])
                child (get query-result [:category/id c-id])]
            (is (= "Phones" (:category/name child)))
            (is (= "Electronics" (get-in child [:category/parent :category/name])))
            (is (= "All Products" (get-in child [:category/parent :category/parent :category/name]))))

          ;; Query from grandparent down through descendants
          (let [query-result (pathom-query env
                                           [{[:category/id gp-id]
                                             [:category/id :category/name
                                              {:category/children
                                               [:category/id :category/name
                                                {:category/children
                                                 [:category/id :category/name]}]}]}])
                gp (get query-result [:category/id gp-id])
                parent (first (:category/children gp))
                child (first (:category/children parent))]
            (is (= "All Products" (:category/name gp)))
            (is (= "Electronics" (:category/name parent)))
            (is (= "Phones" (:category/name child)))))))))
