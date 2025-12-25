(ns ^:integration com.fulcrologic.rad.database-adapters.pg2.example.queries-test
  "Integration tests for the issue tracker example model.

   These tests exercise:
   - 5-level deep Pathom queries
   - Many-to-many relationships via join tables
   - delete-orphan? behavior
   - Custom value transformers
   - Self-referential hierarchies
   - All attribute types"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.pg2 :as rad.pg2]
   [com.fulcrologic.rad.database-adapters.pg2.example.model :as model]
   [com.fulcrologic.rad.database-adapters.pg2.migration :as mig]
   [com.fulcrologic.rad.database-adapters.pg2.read :as read]
   [com.fulcrologic.rad.database-adapters.pg2.write :as write]
   [com.fulcrologic.rad.form :as rad.form]
   [com.fulcrologic.rad.ids :as ids]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.connect.operation :as pco]
   [com.wsscode.pathom3.interface.eql :as p.eql]
   [next.jdbc :as jdbc]
   [pg.pool :as pg.pool]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log])
  (:import
   [java.time Instant]))

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

(def key->attribute (enc/keys-by ::attr/qualified-key model/all-attributes))

(defn generate-test-schema-name []
  (str "test_tracker_" (System/currentTimeMillis) "_" (rand-int 10000)))

(def ^:dynamic *test-env* nil)

(defn with-test-db
  "Run test function within an isolated test schema."
  [f]
  (let [ds (jdbc/get-datasource test-db-config)
        schema-name (generate-test-schema-name)
        jdbc-conn (jdbc/get-connection ds)
        pg2-pool (pg.pool/pool (assoc pg2-config
                                      :pg-params {"search_path" schema-name}))]
    (try
      ;; Create isolated test schema
      (jdbc/execute! jdbc-conn [(str "CREATE SCHEMA " schema-name)])
      (jdbc/execute! jdbc-conn [(str "SET search_path TO " schema-name)])

      ;; Generate schema from attributes
      (let [stmts (mig/automatic-schema :tracker model/all-attributes)]
        (doseq [stmt stmts]
          (try
            (jdbc/execute! jdbc-conn [stmt])
            (catch Exception e
              (log/error "Schema creation failed:" stmt (.getMessage e))))))

      ;; Build Pathom environment
      (let [resolvers (read/generate-resolvers model/all-attributes :tracker)
            env (-> {::pci/indexes (pci/register resolvers)
                     ::rad.pg2/connection-pools {:tracker pg2-pool}
                     ::attr/key->attribute key->attribute}
                    (pci/register resolvers))]
        (binding [*test-env* {:env env
                              :pool pg2-pool
                              :schema schema-name
                              :jdbc-conn jdbc-conn
                              :key->attr key->attribute}]
          (f)))

      (finally
        ;; Cleanup
        (try
          (jdbc/execute! jdbc-conn [(str "DROP SCHEMA " schema-name " CASCADE")])
          (catch Exception _))
        (pg.pool/close pg2-pool)
        (.close jdbc-conn)))))

(use-fixtures :each with-test-db)

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn run-query [query]
  (p.eql/process (:env *test-env*) query))

(defn run-query-with-ident [ident query]
  (p.eql/process (:env *test-env*) ident query))

(defn save! [delta]
  (let [{:keys [pool key->attr]} *test-env*
        params {::rad.form/delta delta
                ::rad.form/params {}}
        env {::rad.pg2/connection-pools {:tracker pool}
             ::attr/key->attribute key->attr}]
    (write/save-form! env params)))

(defn now [] (Instant/now))

;; =============================================================================
;; Test: 5-Level Deep Query
;; Organization → Project → Issue → Comment → Reaction
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/fk-attr
;;
;; Types exercised:
;;   :uuid, :long (sequence ID), :string, :boolean, :enum, :instant, :ref
;;
;; Patterns:
;;   - To-many reverse refs (fk-attr on child)
;;   - Sequence-based ID generation (:long)
;;   - 5-level deep nested query
;; =============================================================================

(deftest five-level-deep-query-test
  (testing "Query traversing 5 levels: Organization → Project → Issue → Comment → Reaction"
    (let [;; Create entities bottom-up
          user-id (ids/new-uuid)
          org-id (ids/new-uuid)
          project-id (ids/new-uuid)
          issue-tempid (tempid/tempid)
          comment-id (ids/new-uuid)
          reaction-id (ids/new-uuid)

          ;; Save user first
          _ (save! {[:user/id user-id]
                    {:user/email {:after "test@example.com"}
                     :user/username {:after "testuser"}
                     :user/active? {:after true}}})

          ;; Save organization
          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Test Org"}
                     :organization/slug {:after "test-org"}
                     :organization/public? {:after true}
                     :organization/created-at {:after (now)}}})

          ;; Save project
          _ (save! {[:project/id project-id]
                    {:project/name {:after "Test Project"}
                     :project/key {:after "TEST"}
                     :project/organization {:after [:organization/id org-id]}
                     :project/created-at {:after (now)}}})

          ;; Save issue (uses :long ID, sequence-generated)
          result (save! {[:issue/id issue-tempid]
                         {:issue/title {:after "Test Issue"}
                          :issue/description {:after "Description"}
                          :issue/status {:after :issue.status/open}
                          :issue/priority {:after :issue.priority/high}
                          :issue/type {:after :issue.type/bug}
                          :issue/project {:after [:project/id project-id]}
                          :issue/reporter {:after [:user/id user-id]}
                          :issue/created-at {:after (now)}}})
          issue-id (get-in result [:tempids issue-tempid])

          ;; Save comment
          _ (save! {[:comment/id comment-id]
                    {:comment/body {:after "This is a comment"}
                     :comment/issue {:after [:issue/id issue-id]}
                     :comment/author {:after [:user/id user-id]}
                     :comment/created-at {:after (now)}}})

          ;; Save reaction
          _ (save! {[:reaction/id reaction-id]
                    {:reaction/type {:after :reaction.type/thumbs-up}
                     :reaction/comment {:after [:comment/id comment-id]}
                     :reaction/user {:after [:user/id user-id]}
                     :reaction/created-at {:after (now)}}})

          ;; Query 5 levels deep
          query-result (run-query-with-ident
                        [:organization/id org-id]
                        [:organization/name
                         {:organization/projects
                          [:project/name
                           {:project/issues
                            [:issue/title
                             {:issue/comments
                              [:comment/body
                               {:comment/reactions
                                [:reaction/type]}]}]}]}])]

      (is (= "Test Org" (:organization/name query-result)))
      (is (= 1 (count (:organization/projects query-result))))
      (is (= "Test Project" (-> query-result :organization/projects first :project/name)))
      (is (= 1 (-> query-result :organization/projects first :project/issues count)))
      (is (= "Test Issue" (-> query-result :organization/projects first :project/issues first :issue/title)))
      (is (= "This is a comment"
             (-> query-result :organization/projects first :project/issues first
                 :issue/comments first :comment/body)))
      (is (= :reaction.type/thumbs-up
             (-> query-result :organization/projects first :project/issues first
                 :issue/comments first :comment/reactions first :reaction/type))))))

;; =============================================================================
;; Test: Many-to-Many Issue ↔ Labels
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/fk-attr, ::pg2/order-by, ::pg2/max-length
;;
;; Types exercised:
;;   :uuid, :long, :string, :keyword, :int, :enum, :instant, :ref
;;
;; Patterns:
;;   - Many-to-many relationship via join table (IssueLabel)
;;   - Join table with metadata (added-at timestamp)
;;   - :keyword type for color (Label)
;;   - :int type for position ordering (Label)
;; =============================================================================

(deftest many-to-many-issue-labels-test
  (testing "Issue ↔ Label many-to-many relationship"
    (let [user-id (ids/new-uuid)
          org-id (ids/new-uuid)
          project-id (ids/new-uuid)
          label1-id (ids/new-uuid)
          label2-id (ids/new-uuid)
          issue-tempid (tempid/tempid)
          issue-label1-id (ids/new-uuid)
          issue-label2-id (ids/new-uuid)

          ;; Setup
          _ (save! {[:user/id user-id]
                    {:user/email {:after "test@example.com"}
                     :user/username {:after "testuser"}
                     :user/active? {:after true}}})
          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Test Org"}}})
          _ (save! {[:project/id project-id]
                    {:project/name {:after "Test Project"}
                     :project/organization {:after [:organization/id org-id]}}})

          ;; Create labels
          _ (save! {[:label/id label1-id]
                    {:label/name {:after "bug"}
                     :label/color {:after :color/red}
                     :label/position {:after 1}
                     :label/project {:after [:project/id project-id]}}})
          _ (save! {[:label/id label2-id]
                    {:label/name {:after "enhancement"}
                     :label/color {:after :color/blue}
                     :label/position {:after 2}
                     :label/project {:after [:project/id project-id]}}})

          ;; Create issue
          result (save! {[:issue/id issue-tempid]
                         {:issue/title {:after "Labeled Issue"}
                          :issue/status {:after :issue.status/open}
                          :issue/type {:after :issue.type/bug}
                          :issue/project {:after [:project/id project-id]}
                          :issue/reporter {:after [:user/id user-id]}
                          :issue/created-at {:after (now)}}})
          issue-id (get-in result [:tempids issue-tempid])

          ;; Create issue-label associations (M:M join records)
          _ (save! {[:issue-label/id issue-label1-id]
                    {:issue-label/issue {:after [:issue/id issue-id]}
                     :issue-label/label {:after [:label/id label1-id]}
                     :issue-label/added-at {:after (now)}}})
          _ (save! {[:issue-label/id issue-label2-id]
                    {:issue-label/issue {:after [:issue/id issue-id]}
                     :issue-label/label {:after [:label/id label2-id]}
                     :issue-label/added-at {:after (now)}}})

          ;; Query issue with labels through join table
          query-result (run-query-with-ident
                        [:issue/id issue-id]
                        [:issue/title
                         {:issue/labels
                          [:issue-label/id
                           {:issue-label/label
                            [:label/name :label/color]}]}])]

      (is (= "Labeled Issue" (:issue/title query-result)))
      (is (= 2 (count (:issue/labels query-result))))
      (let [label-names (->> query-result :issue/labels
                             (map #(-> % :issue-label/label :label/name))
                             set)]
        (is (= #{"bug" "enhancement"} label-names))))))

;; =============================================================================
;; Test: Many-to-Many Issue ↔ Assignees
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/fk-attr
;;
;; Types exercised:
;;   :uuid, :long, :string, :boolean, :enum, :instant, :ref
;;
;; Patterns:
;;   - Many-to-many relationship via join table (IssueAssignee)
;;   - Join table with metadata (primary? flag, assigned-at)
;;   - :boolean in join table for primary assignee flag
;; =============================================================================

(deftest many-to-many-issue-assignees-test
  (testing "Issue ↔ User assignees many-to-many relationship"
    (let [user1-id (ids/new-uuid)
          user2-id (ids/new-uuid)
          org-id (ids/new-uuid)
          project-id (ids/new-uuid)
          issue-tempid (tempid/tempid)
          assignee1-id (ids/new-uuid)
          assignee2-id (ids/new-uuid)

          ;; Setup users
          _ (save! {[:user/id user1-id]
                    {:user/email {:after "alice@example.com"}
                     :user/username {:after "alice"}
                     :user/active? {:after true}}})
          _ (save! {[:user/id user2-id]
                    {:user/email {:after "bob@example.com"}
                     :user/username {:after "bob"}
                     :user/active? {:after true}}})
          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Test Org"}}})
          _ (save! {[:project/id project-id]
                    {:project/name {:after "Test Project"}
                     :project/organization {:after [:organization/id org-id]}}})

          ;; Create issue
          result (save! {[:issue/id issue-tempid]
                         {:issue/title {:after "Multi-Assignee Issue"}
                          :issue/status {:after :issue.status/open}
                          :issue/type {:after :issue.type/task}
                          :issue/project {:after [:project/id project-id]}
                          :issue/reporter {:after [:user/id user1-id]}
                          :issue/created-at {:after (now)}}})
          issue-id (get-in result [:tempids issue-tempid])

          ;; Assign both users
          _ (save! {[:issue-assignee/id assignee1-id]
                    {:issue-assignee/issue {:after [:issue/id issue-id]}
                     :issue-assignee/user {:after [:user/id user1-id]}
                     :issue-assignee/primary? {:after true}
                     :issue-assignee/assigned-at {:after (now)}}})
          _ (save! {[:issue-assignee/id assignee2-id]
                    {:issue-assignee/issue {:after [:issue/id issue-id]}
                     :issue-assignee/user {:after [:user/id user2-id]}
                     :issue-assignee/primary? {:after false}
                     :issue-assignee/assigned-at {:after (now)}}})

          ;; Query
          query-result (run-query-with-ident
                        [:issue/id issue-id]
                        [:issue/title
                         {:issue/assignees
                          [:issue-assignee/primary?
                           {:issue-assignee/user
                            [:user/username]}]}])]

      (is (= "Multi-Assignee Issue" (:issue/title query-result)))
      (is (= 2 (count (:issue/assignees query-result))))
      (let [usernames (->> query-result :issue/assignees
                           (map #(-> % :issue-assignee/user :user/username))
                           set)]
        (is (= #{"alice" "bob"} usernames)))
      ;; Check primary assignee
      (let [primary (->> query-result :issue/assignees
                         (filter :issue-assignee/primary?)
                         first)]
        (is (= "alice" (-> primary :issue-assignee/user :user/username)))))))

;; =============================================================================
;; Test: Self-Referential Hierarchy
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/fk-attr, ::pg2/order-by
;;
;; Types exercised:
;;   :uuid, :long, :string, :boolean, :enum, :instant, :int, :ref
;;
;; Patterns:
;;   - Self-referential to-one (issue/parent)
;;   - Self-referential to-many (issue/children)
;;   - ::pg2/order-by on self-referential collection (:issue/priority-order)
;;   - Bidirectional navigation (parent → children, child → parent)
;; =============================================================================

(deftest self-referential-issue-hierarchy-test
  (testing "Issue parent/children self-referential hierarchy"
    (let [user-id (ids/new-uuid)
          org-id (ids/new-uuid)
          project-id (ids/new-uuid)
          parent-tempid (tempid/tempid)
          child1-tempid (tempid/tempid)
          child2-tempid (tempid/tempid)

          ;; Setup
          _ (save! {[:user/id user-id]
                    {:user/email {:after "test@example.com"}
                     :user/username {:after "testuser"}
                     :user/active? {:after true}}})
          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Test Org"}}})
          _ (save! {[:project/id project-id]
                    {:project/name {:after "Test Project"}
                     :project/organization {:after [:organization/id org-id]}}})

          ;; Create parent issue (epic)
          parent-result (save! {[:issue/id parent-tempid]
                                {:issue/title {:after "Epic Issue"}
                                 :issue/status {:after :issue.status/open}
                                 :issue/type {:after :issue.type/epic}
                                 :issue/priority-order {:after 1}
                                 :issue/project {:after [:project/id project-id]}
                                 :issue/reporter {:after [:user/id user-id]}
                                 :issue/created-at {:after (now)}}})
          parent-id (get-in parent-result [:tempids parent-tempid])

          ;; Create child issues
          child1-result (save! {[:issue/id child1-tempid]
                                {:issue/title {:after "Child Task 1"}
                                 :issue/status {:after :issue.status/open}
                                 :issue/type {:after :issue.type/task}
                                 :issue/priority-order {:after 1}
                                 :issue/project {:after [:project/id project-id]}
                                 :issue/reporter {:after [:user/id user-id]}
                                 :issue/parent {:after [:issue/id parent-id]}
                                 :issue/created-at {:after (now)}}})
          child1-id (get-in child1-result [:tempids child1-tempid])

          child2-result (save! {[:issue/id child2-tempid]
                                {:issue/title {:after "Child Task 2"}
                                 :issue/status {:after :issue.status/open}
                                 :issue/type {:after :issue.type/task}
                                 :issue/priority-order {:after 2}
                                 :issue/project {:after [:project/id project-id]}
                                 :issue/reporter {:after [:user/id user-id]}
                                 :issue/parent {:after [:issue/id parent-id]}
                                 :issue/created-at {:after (now)}}})

          ;; Query parent with children
          parent-query (run-query-with-ident
                        [:issue/id parent-id]
                        [:issue/title
                         :issue/type
                         {:issue/children
                          [:issue/title :issue/priority-order]}])

          ;; Query child with parent
          child-query (run-query-with-ident
                       [:issue/id child1-id]
                       [:issue/title
                        {:issue/parent
                         [:issue/title :issue/type]}])]

      ;; Parent has children
      (is (= "Epic Issue" (:issue/title parent-query)))
      (is (= :issue.type/epic (:issue/type parent-query)))
      (is (= 2 (count (:issue/children parent-query))))

      ;; Children are ordered by priority-order
      (is (= ["Child Task 1" "Child Task 2"]
             (mapv :issue/title (:issue/children parent-query))))

      ;; Child has parent
      (is (= "Child Task 1" (:issue/title child-query)))
      (is (= "Epic Issue" (-> child-query :issue/parent :issue/title)))
      (is (= :issue.type/epic (-> child-query :issue/parent :issue/type))))))

;; =============================================================================
;; Test: Custom Value Transformer - JSON
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/max-length,
;;   ::pg2/form->sql-value, ::pg2/sql->form-value
;;
;; Types exercised:
;;   :uuid, :string, :password, :instant, :ref
;;
;; Patterns:
;;   - Custom transformer pair for JSON serialization (ApiToken permissions)
;;   - Map data stored as JSON string in PostgreSQL
;;   - Round-trip verification (Clojure → JSON string → Clojure)
;; =============================================================================

(deftest custom-transformer-json-test
  (testing "Custom JSON transformer for API token permissions"
    (let [user-id (ids/new-uuid)
          token-id (ids/new-uuid)
          permissions {:read true :write true :admin false}

          _ (save! {[:user/id user-id]
                    {:user/email {:after "test@example.com"}
                     :user/username {:after "testuser"}
                     :user/active? {:after true}}})

          _ (save! {[:api-token/id token-id]
                    {:api-token/name {:after "CI Token"}
                     :api-token/token-hash {:after "hashed_secret"}
                     :api-token/permissions {:after permissions}
                     :api-token/user {:after [:user/id user-id]}
                     :api-token/expires-at {:after (now)}}})

          query-result (run-query-with-ident
                        [:api-token/id token-id]
                        [:api-token/name :api-token/permissions])]

      (is (= "CI Token" (:api-token/name query-result)))
      ;; Permissions should be decoded back to map
      (is (= permissions (:api-token/permissions query-result))))))

;; =============================================================================
;; Test: Custom Value Transformer - CSV Tags
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/max-length,
;;   ::pg2/form->sql-value, ::pg2/sql->form-value
;;
;; Types exercised:
;;   :uuid, :string, :boolean, :password, :ref
;;
;; Patterns:
;;   - Custom transformer pair for CSV tag serialization (Webhook events)
;;   - Vector of keywords stored as comma-separated string
;;   - Round-trip verification (keywords → CSV string → keywords)
;; =============================================================================

(deftest custom-transformer-csv-tags-test
  (testing "Custom CSV transformer for webhook events"
    (let [org-id (ids/new-uuid)
          project-id (ids/new-uuid)
          webhook-id (ids/new-uuid)
          events [:issue-created :issue-closed :comment-added]

          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Test Org"}}})
          _ (save! {[:project/id project-id]
                    {:project/name {:after "Test Project"}
                     :project/organization {:after [:organization/id org-id]}}})

          _ (save! {[:webhook/id webhook-id]
                    {:webhook/name {:after "CI Webhook"}
                     :webhook/url {:after "https://ci.example.com/hook"}
                     :webhook/active? {:after true}
                     :webhook/events {:after events}
                     :webhook/project {:after [:project/id project-id]}}})

          query-result (run-query-with-ident
                        [:webhook/id webhook-id]
                        [:webhook/name :webhook/events])]

      (is (= "CI Webhook" (:webhook/name query-result)))
      ;; Events should be decoded back to vector of keywords
      (is (= events (:webhook/events query-result))))))

;; =============================================================================
;; Test: delete-orphan? - API Tokens
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/fk-attr, ::pg2/delete-orphan?
;;
;; Types exercised:
;;   :uuid, :string, :boolean, :password, :ref
;;
;; Patterns:
;;   - Owned component relationship (User owns ApiTokens)
;;   - Cascade delete when reference removed from parent's collection
;;   - Verifies entity is deleted from database (not just unlinked)
;; =============================================================================

(deftest delete-orphan-api-tokens-test
  (testing "delete-orphan? removes API tokens when removed from user"
    (let [user-id (ids/new-uuid)
          token-id (ids/new-uuid)

          _ (save! {[:user/id user-id]
                    {:user/email {:after "test@example.com"}
                     :user/username {:after "testuser"}
                     :user/active? {:after true}}})

          _ (save! {[:api-token/id token-id]
                    {:api-token/name {:after "Test Token"}
                     :api-token/token-hash {:after "secret"}
                     :api-token/user {:after [:user/id user-id]}}})

          ;; Verify token exists
          before-query (run-query-with-ident
                        [:api-token/id token-id]
                        [:api-token/name])
          _ (is (= "Test Token" (:api-token/name before-query)))

          ;; Remove token from user (should trigger delete-orphan)
          _ (save! {[:user/id user-id]
                    {:user/api-tokens {:before [[:api-token/id token-id]]
                                       :after []}}})

          ;; Token should be deleted
          after-query (run-query-with-ident
                       [:api-token/id token-id]
                       [:api-token/name])]

      (is (nil? (:api-token/name after-query))))))

;; =============================================================================
;; Test: delete-orphan? - Comment Reactions
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/fk-attr, ::pg2/delete-orphan?
;;
;; Types exercised:
;;   :uuid, :long, :string, :boolean, :enum, :instant, :ref
;;
;; Patterns:
;;   - Owned component in nested hierarchy (Comment → Reaction at level 5)
;;   - Cascade delete on deeply nested entity
;;   - delete-orphan? on to-many with fk-attr
;; =============================================================================

(deftest delete-orphan-comment-reactions-test
  (testing "delete-orphan? removes reactions when removed from comment"
    (let [user-id (ids/new-uuid)
          org-id (ids/new-uuid)
          project-id (ids/new-uuid)
          issue-tempid (tempid/tempid)
          comment-id (ids/new-uuid)
          reaction-id (ids/new-uuid)

          _ (save! {[:user/id user-id]
                    {:user/email {:after "test@example.com"}
                     :user/username {:after "testuser"}
                     :user/active? {:after true}}})
          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Test Org"}}})
          _ (save! {[:project/id project-id]
                    {:project/name {:after "Test Project"}
                     :project/organization {:after [:organization/id org-id]}}})

          issue-result (save! {[:issue/id issue-tempid]
                               {:issue/title {:after "Test Issue"}
                                :issue/status {:after :issue.status/open}
                                :issue/type {:after :issue.type/bug}
                                :issue/project {:after [:project/id project-id]}
                                :issue/reporter {:after [:user/id user-id]}
                                :issue/created-at {:after (now)}}})
          issue-id (get-in issue-result [:tempids issue-tempid])

          _ (save! {[:comment/id comment-id]
                    {:comment/body {:after "Test comment"}
                     :comment/issue {:after [:issue/id issue-id]}
                     :comment/author {:after [:user/id user-id]}
                     :comment/created-at {:after (now)}}})

          _ (save! {[:reaction/id reaction-id]
                    {:reaction/type {:after :reaction.type/heart}
                     :reaction/comment {:after [:comment/id comment-id]}
                     :reaction/user {:after [:user/id user-id]}
                     :reaction/created-at {:after (now)}}})

          ;; Verify reaction exists
          before-query (run-query-with-ident
                        [:reaction/id reaction-id]
                        [:reaction/type])
          _ (is (= :reaction.type/heart (:reaction/type before-query)))

          ;; Remove reaction from comment
          _ (save! {[:comment/id comment-id]
                    {:comment/reactions {:before [[:reaction/id reaction-id]]
                                         :after []}}})

          ;; Reaction should be deleted
          after-query (run-query-with-ident
                       [:reaction/id reaction-id]
                       [:reaction/type])]

      (is (nil? (:reaction/type after-query))))))

;; =============================================================================
;; Test: All Attribute Types Round-Trip
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name
;;
;; Types exercised (ALL supported types):
;;   :uuid     - Organization, Project, User IDs
;;   :long     - Issue ID (sequence-generated)
;;   :int      - priority-order, vote-count
;;   :string   - title, description, email
;;   :password - password-hash
;;   :boolean  - active?, admin?
;;   :decimal  - estimate, time-spent, budget
;;   :instant  - created-at, due-date, last-login-at
;;   :enum     - status, priority, type
;;   :symbol   - workflow-state, default-workflow
;;   :ref      - to-one relationships (project, reporter)
;;
;; Patterns:
;;   - Comprehensive type round-trip verification
;;   - All scalar types written and read back correctly
;;   - Nested refs verify types at multiple levels
;; =============================================================================

(deftest all-attribute-types-test
  (testing "All attribute types round-trip correctly"
    (let [user-id (ids/new-uuid)
          org-id (ids/new-uuid)
          project-id (ids/new-uuid)
          issue-tempid (tempid/tempid)
          now-instant (now)
          test-estimate 42.5M

          _ (save! {[:user/id user-id]
                    {:user/email {:after "test@example.com"}
                     :user/username {:after "testuser"}
                     :user/display-name {:after "Test User"}
                     :user/password-hash {:after "bcrypt_hash_here"}
                     :user/active? {:after true}
                     :user/admin? {:after false}
                     :user/created-at {:after now-instant}
                     :user/last-login-at {:after now-instant}}})

          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Test Org"}}})

          _ (save! {[:project/id project-id]
                    {:project/name {:after "Test Project"}
                     :project/key {:after "TEST"}
                     :project/default-workflow {:after 'workflow/standard}
                     :project/budget {:after 10000.00M}
                     :project/organization {:after [:organization/id org-id]}}})

          result (save! {[:issue/id issue-tempid]
                         {:issue/title {:after "Type Test Issue"}
                          :issue/description {:after "Testing all types"}
                          :issue/status {:after :issue.status/in-progress}
                          :issue/priority {:after :issue.priority/critical}
                          :issue/type {:after :issue.type/feature}
                          :issue/workflow-state {:after 'state/active}
                          :issue/priority-order {:after 42}
                          :issue/estimate {:after test-estimate}
                          :issue/time-spent {:after 10.25M}
                          :issue/vote-count {:after 100}
                          :issue/created-at {:after now-instant}
                          :issue/due-date {:after now-instant}
                          :issue/project {:after [:project/id project-id]}
                          :issue/reporter {:after [:user/id user-id]}}})
          issue-id (get-in result [:tempids issue-tempid])

          ;; Query all fields
          query-result (run-query-with-ident
                        [:issue/id issue-id]
                        [:issue/id                ; :long
                         :issue/title             ; :string
                         :issue/status            ; :enum
                         :issue/priority          ; :enum
                         :issue/type              ; :enum
                         :issue/workflow-state    ; :symbol
                         :issue/priority-order    ; :int
                         :issue/estimate          ; :decimal
                         :issue/time-spent        ; :decimal
                         :issue/vote-count        ; :int
                         :issue/created-at        ; :instant
                         {:issue/project          ; :ref
                          [:project/budget        ; :decimal
                           :project/default-workflow]}  ; :symbol
                         {:issue/reporter         ; :ref
                          [:user/active?          ; :boolean
                           :user/password-hash]}])] ; :password

      ;; Verify types
      (is (int? (:issue/id query-result)))                        ; :long (sequence)
      (is (string? (:issue/title query-result)))                  ; :string
      (is (keyword? (:issue/status query-result)))                ; :enum
      (is (= :issue.status/in-progress (:issue/status query-result)))
      (is (keyword? (:issue/priority query-result)))              ; :enum
      (is (keyword? (:issue/type query-result)))                  ; :enum
      (is (symbol? (:issue/workflow-state query-result)))         ; :symbol
      (is (= 'state/active (:issue/workflow-state query-result)))
      (is (int? (:issue/priority-order query-result)))            ; :int
      (is (= 42 (:issue/priority-order query-result)))
      (is (decimal? (:issue/estimate query-result)))              ; :decimal
      (is (= test-estimate (:issue/estimate query-result)))
      (is (int? (:issue/vote-count query-result)))                ; :int
      (is (= 100 (:issue/vote-count query-result)))
      (is (instance? Instant (:issue/created-at query-result)))   ; :instant

      ;; Nested refs
      (is (decimal? (-> query-result :issue/project :project/budget)))
      (is (symbol? (-> query-result :issue/project :project/default-workflow)))
      (is (boolean? (-> query-result :issue/reporter :user/active?)))
      (is (string? (-> query-result :issue/reporter :user/password-hash))))))

;; =============================================================================
;; Test: order-by on Collections
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/fk-attr, ::pg2/order-by
;;
;; Types exercised:
;;   :uuid, :string, :keyword, :int, :ref
;;
;; Patterns:
;;   - Ordered to-many collection (project/labels ordered by :label/position)
;;   - Verifies order is preserved regardless of insertion order
;;   - :int type used for explicit ordering
;; =============================================================================

(deftest order-by-labels-test
  (testing "Labels are ordered by position"
    (let [org-id (ids/new-uuid)
          project-id (ids/new-uuid)
          label1-id (ids/new-uuid)
          label2-id (ids/new-uuid)
          label3-id (ids/new-uuid)

          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Test Org"}}})
          _ (save! {[:project/id project-id]
                    {:project/name {:after "Test Project"}
                     :project/organization {:after [:organization/id org-id]}}})

          ;; Create labels out of order
          _ (save! {[:label/id label2-id]
                    {:label/name {:after "Beta"}
                     :label/position {:after 2}
                     :label/project {:after [:project/id project-id]}}})
          _ (save! {[:label/id label3-id]
                    {:label/name {:after "Gamma"}
                     :label/position {:after 3}
                     :label/project {:after [:project/id project-id]}}})
          _ (save! {[:label/id label1-id]
                    {:label/name {:after "Alpha"}
                     :label/position {:after 1}
                     :label/project {:after [:project/id project-id]}}})

          ;; Query labels - should be ordered by position
          query-result (run-query-with-ident
                        [:project/id project-id]
                        [:project/name
                         {:project/labels
                          [:label/name :label/position]}])]

      (is (= ["Alpha" "Beta" "Gamma"]
             (mapv :label/name (:project/labels query-result)))))))

;; =============================================================================
;; Test: Alternative 5-Level Path
;; Organization → Team → TeamMember → User → Notification
;;
;; Options exercised:
;;   ::pg2/table, ::pg2/column-name, ::pg2/fk-attr, ::pg2/order-by
;;
;; Types exercised:
;;   :uuid, :string, :boolean, :enum, :instant, :ref
;;
;; Patterns:
;;   - Alternative 5-level deep query path (not via Issue/Comment)
;;   - Many-to-many with role (Team ↔ User via TeamMember)
;;   - Ordered to-many (notifications ordered by created-at)
;;   - Traversing through join table with metadata (role, joined-at)
;; =============================================================================

(deftest alternative-five-level-path-test
  (testing "Query: Organization → Team → TeamMember → User → Notification"
    (let [user-id (ids/new-uuid)
          org-id (ids/new-uuid)
          team-id (ids/new-uuid)
          team-member-id (ids/new-uuid)
          notification-id (ids/new-uuid)

          ;; Create user
          _ (save! {[:user/id user-id]
                    {:user/email {:after "team@example.com"}
                     :user/username {:after "teamuser"}
                     :user/active? {:after true}}})

          ;; Create notification for user
          _ (save! {[:notification/id notification-id]
                    {:notification/title {:after "You were added to a team"}
                     :notification/type {:after :notification.type/project-invited}
                     :notification/read? {:after false}
                     :notification/user {:after [:user/id user-id]}
                     :notification/created-at {:after (now)}}})

          ;; Create organization
          _ (save! {[:organization/id org-id]
                    {:organization/name {:after "Team Org"}
                     :organization/created-at {:after (now)}}})

          ;; Create team
          _ (save! {[:team/id team-id]
                    {:team/name {:after "Engineering"}
                     :team/organization {:after [:organization/id org-id]}}})

          ;; Add user to team via TeamMember
          _ (save! {[:team-member/id team-member-id]
                    {:team-member/role {:after :team.role/member}
                     :team-member/team {:after [:team/id team-id]}
                     :team-member/user {:after [:user/id user-id]}
                     :team-member/joined-at {:after (now)}}})

          ;; Query 5 levels
          query-result (run-query-with-ident
                        [:organization/id org-id]
                        [:organization/name
                         {:organization/teams
                          [:team/name
                           {:team/members
                            [:team-member/role
                             {:team-member/user
                              [:user/username
                               {:user/notifications
                                [:notification/title :notification/read?]}]}]}]}])]

      (is (= "Team Org" (:organization/name query-result)))
      (is (= "Engineering"
             (-> query-result :organization/teams first :team/name)))
      (is (= :team.role/member
             (-> query-result :organization/teams first :team/members first :team-member/role)))
      (is (= "teamuser"
             (-> query-result :organization/teams first :team/members first
                 :team-member/user :user/username)))
      (is (= "You were added to a team"
             (-> query-result :organization/teams first :team/members first
                 :team-member/user :user/notifications first :notification/title))))))
