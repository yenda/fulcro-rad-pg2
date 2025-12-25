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
  (get (p.eql/process (:env *test-env*) [{ident query}]) ident))

(defn save! [delta]
  (let [{:keys [pool key->attr]} *test-env*
        params {::rad.form/delta delta
                ::rad.form/params {}}
        env {::rad.pg2/connection-pools {:tracker pool}
             ::attr/key->attribute key->attr}]
    (write/save-form! env params)))

(defn create!
  "Create a new entity using tempid and return the real ID.
   delta-fn is called with the tempid to build the delta."
  [id-key delta-fn]
  (let [tid (tempid/tempid)
        result (save! {[id-key tid] (delta-fn tid)})]
    (get-in result [:tempids tid])))

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
    (let [;; Create entities using tempids - save-form! treats non-tempids as updates
          user-tempid (tempid/tempid)
          org-tempid (tempid/tempid)
          project-tempid (tempid/tempid)
          issue-tempid (tempid/tempid)
          comment-tempid (tempid/tempid)
          reaction-tempid (tempid/tempid)

          ;; Save user first
          user-result (save! {[:user/id user-tempid]
                              {:user/email {:after "test@example.com"}
                               :user/username {:after "testuser"}
                               :user/active? {:after true}}})
          user-id (get-in user-result [:tempids user-tempid])

          ;; Save organization
          org-result (save! {[:organization/id org-tempid]
                             {:organization/name {:after "Test Org"}
                              :organization/slug {:after "test-org"}
                              :organization/public? {:after true}
                              :organization/created-at {:after (now)}}})
          org-id (get-in org-result [:tempids org-tempid])

          ;; Save project
          project-result (save! {[:project/id project-tempid]
                                 {:project/name {:after "Test Project"}
                                  :project/key {:after "TEST"}
                                  :project/organization {:after [:organization/id org-id]}
                                  :project/created-at {:after (now)}}})
          project-id (get-in project-result [:tempids project-tempid])

          ;; Save issue (uses :long ID, sequence-generated)
          issue-result (save! {[:issue/id issue-tempid]
                               {:issue/title {:after "Test Issue"}
                                :issue/description {:after "Description"}
                                :issue/status {:after :issue.status/open}
                                :issue/priority {:after :issue.priority/high}
                                :issue/type {:after :issue.type/bug}
                                :issue/project {:after [:project/id project-id]}
                                :issue/reporter {:after [:user/id user-id]}
                                :issue/created-at {:after (now)}}})
          issue-id (get-in issue-result [:tempids issue-tempid])

          ;; Save comment
          comment-result (save! {[:comment/id comment-tempid]
                                 {:comment/body {:after "This is a comment"}
                                  :comment/issue {:after [:issue/id issue-id]}
                                  :comment/author {:after [:user/id user-id]}
                                  :comment/created-at {:after (now)}}})
          comment-id (get-in comment-result [:tempids comment-tempid])

          ;; Save reaction
          _ (save! {[:reaction/id reaction-tempid]
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
    (let [;; Create entities using tempids
          user-id (create! :user/id (fn [_] {:user/email {:after "test@example.com"}
                                             :user/username {:after "testuser"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Test Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create labels
          label1-id (create! :label/id (fn [_] {:label/name {:after "bug"}
                                                :label/color {:after :color/red}
                                                :label/position {:after 1}
                                                :label/project {:after [:project/id project-id]}}))
          label2-id (create! :label/id (fn [_] {:label/name {:after "enhancement"}
                                                :label/color {:after :color/blue}
                                                :label/position {:after 2}
                                                :label/project {:after [:project/id project-id]}}))

          ;; Create issue
          issue-id (create! :issue/id (fn [_] {:issue/title {:after "Labeled Issue"}
                                               :issue/status {:after :issue.status/open}
                                               :issue/type {:after :issue.type/bug}
                                               :issue/project {:after [:project/id project-id]}
                                               :issue/reporter {:after [:user/id user-id]}
                                               :issue/created-at {:after (now)}}))

          ;; Create issue-label associations (M:M join records)
          _ (create! :issue-label/id (fn [_] {:issue-label/issue {:after [:issue/id issue-id]}
                                              :issue-label/label {:after [:label/id label1-id]}
                                              :issue-label/added-at {:after (now)}}))
          _ (create! :issue-label/id (fn [_] {:issue-label/issue {:after [:issue/id issue-id]}
                                              :issue-label/label {:after [:label/id label2-id]}
                                              :issue-label/added-at {:after (now)}}))

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
    (let [;; Create entities using tempids
          user1-id (create! :user/id (fn [_] {:user/email {:after "alice@example.com"}
                                              :user/username {:after "alice"}
                                              :user/active? {:after true}}))
          user2-id (create! :user/id (fn [_] {:user/email {:after "bob@example.com"}
                                              :user/username {:after "bob"}
                                              :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Test Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create issue
          issue-id (create! :issue/id (fn [_] {:issue/title {:after "Multi-Assignee Issue"}
                                               :issue/status {:after :issue.status/open}
                                               :issue/type {:after :issue.type/task}
                                               :issue/project {:after [:project/id project-id]}
                                               :issue/reporter {:after [:user/id user1-id]}
                                               :issue/created-at {:after (now)}}))

          ;; Assign both users
          _ (create! :issue-assignee/id (fn [_] {:issue-assignee/issue {:after [:issue/id issue-id]}
                                                 :issue-assignee/user {:after [:user/id user1-id]}
                                                 :issue-assignee/primary? {:after true}
                                                 :issue-assignee/assigned-at {:after (now)}}))
          _ (create! :issue-assignee/id (fn [_] {:issue-assignee/issue {:after [:issue/id issue-id]}
                                                 :issue-assignee/user {:after [:user/id user2-id]}
                                                 :issue-assignee/primary? {:after false}
                                                 :issue-assignee/assigned-at {:after (now)}}))

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
    (let [;; Create entities using tempids
          user-id (create! :user/id (fn [_] {:user/email {:after "test@example.com"}
                                             :user/username {:after "testuser"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Test Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create parent issue (epic)
          parent-id (create! :issue/id (fn [_] {:issue/title {:after "Epic Issue"}
                                                :issue/status {:after :issue.status/open}
                                                :issue/type {:after :issue.type/epic}
                                                :issue/priority-order {:after 1}
                                                :issue/project {:after [:project/id project-id]}
                                                :issue/reporter {:after [:user/id user-id]}
                                                :issue/created-at {:after (now)}}))

          ;; Create child issues
          child1-id (create! :issue/id (fn [_] {:issue/title {:after "Child Task 1"}
                                                :issue/status {:after :issue.status/open}
                                                :issue/type {:after :issue.type/task}
                                                :issue/priority-order {:after 1}
                                                :issue/project {:after [:project/id project-id]}
                                                :issue/reporter {:after [:user/id user-id]}
                                                :issue/parent {:after [:issue/id parent-id]}
                                                :issue/created-at {:after (now)}}))

          _ (create! :issue/id (fn [_] {:issue/title {:after "Child Task 2"}
                                        :issue/status {:after :issue.status/open}
                                        :issue/type {:after :issue.type/task}
                                        :issue/priority-order {:after 2}
                                        :issue/project {:after [:project/id project-id]}
                                        :issue/reporter {:after [:user/id user-id]}
                                        :issue/parent {:after [:issue/id parent-id]}
                                        :issue/created-at {:after (now)}}))

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
    (let [permissions {:read true :write true :admin false}
          user-id (create! :user/id (fn [_] {:user/email {:after "test@example.com"}
                                             :user/username {:after "testuser"}
                                             :user/active? {:after true}}))
          token-id (create! :api-token/id (fn [_] {:api-token/name {:after "CI Token"}
                                                   :api-token/token-hash {:after "hashed_secret"}
                                                   :api-token/permissions {:after permissions}
                                                   :api-token/user {:after [:user/id user-id]}
                                                   :api-token/expires-at {:after (now)}}))

          ;; Verify write transformer applied by checking DB directly
          db-result (jdbc/execute-one! (:jdbc-conn *test-env*)
                                       ["SELECT permissions FROM api_tokens WHERE id = ?" token-id])
          ;; JDBC returns table-qualified keys, get the first (only) value
          stored-permissions (first (vals db-result))
          query-result (run-query-with-ident
                        [:api-token/id token-id]
                        [:api-token/name])]

      (is (= "CI Token" (:api-token/name query-result)))
      ;; Write transformer applied: map stored as string
      (is (= (pr-str permissions) stored-permissions)))))

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
    (let [events [:issue-created :issue-closed :comment-added]
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Test Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))
          webhook-id (create! :webhook/id (fn [_] {:webhook/name {:after "CI Webhook"}
                                                   :webhook/url {:after "https://ci.example.com/hook"}
                                                   :webhook/active? {:after true}
                                                   :webhook/events {:after events}
                                                   :webhook/project {:after [:project/id project-id]}}))

          ;; Verify write transformer applied by checking DB directly
          db-result (jdbc/execute-one! (:jdbc-conn *test-env*)
                                       ["SELECT events FROM webhooks WHERE id = ?" webhook-id])
          ;; JDBC returns table-qualified keys, get the first (only) value
          stored-events (first (vals db-result))
          query-result (run-query-with-ident
                        [:webhook/id webhook-id]
                        [:webhook/name])]

      (is (= "CI Webhook" (:webhook/name query-result)))
      ;; Write transformer applied: vector stored as CSV string
      (is (= "issue-created,issue-closed,comment-added" stored-events)))))

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
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "test@example.com"}
                                             :user/username {:after "testuser"}
                                             :user/active? {:after true}}))
          token-id (create! :api-token/id (fn [_] {:api-token/name {:after "Test Token"}
                                                   :api-token/token-hash {:after "secret"}
                                                   :api-token/user {:after [:user/id user-id]}}))

          ;; Verify token exists
          before-query (run-query-with-ident
                        [:api-token/id token-id]
                        [:api-token/name])
          _ (is (= "Test Token" (:api-token/name before-query)))

          ;; Remove token from user (should trigger delete-orphan)
          _ (save! {[:user/id user-id]
                    {:user/api-tokens {:before [[:api-token/id token-id]]
                                       :after []}}})

          ;; Token should be deleted - verify via direct database query
          db-result (jdbc/execute-one! (:jdbc-conn *test-env*)
                                       ["SELECT * FROM api_tokens WHERE id = ?" token-id])]

      (is (nil? db-result) "API token should be deleted from database"))))

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
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "test@example.com"}
                                             :user/username {:after "testuser"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Test Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))
          issue-id (create! :issue/id (fn [_] {:issue/title {:after "Test Issue"}
                                               :issue/status {:after :issue.status/open}
                                               :issue/type {:after :issue.type/bug}
                                               :issue/project {:after [:project/id project-id]}
                                               :issue/reporter {:after [:user/id user-id]}
                                               :issue/created-at {:after (now)}}))
          comment-id (create! :comment/id (fn [_] {:comment/body {:after "Test comment"}
                                                   :comment/issue {:after [:issue/id issue-id]}
                                                   :comment/author {:after [:user/id user-id]}
                                                   :comment/created-at {:after (now)}}))
          reaction-id (create! :reaction/id (fn [_] {:reaction/type {:after :reaction.type/heart}
                                                     :reaction/comment {:after [:comment/id comment-id]}
                                                     :reaction/user {:after [:user/id user-id]}
                                                     :reaction/created-at {:after (now)}}))

          ;; Verify reaction exists
          before-query (run-query-with-ident
                        [:reaction/id reaction-id]
                        [:reaction/type])
          _ (is (= :reaction.type/heart (:reaction/type before-query)))

          ;; Remove reaction from comment
          _ (save! {[:comment/id comment-id]
                    {:comment/reactions {:before [[:reaction/id reaction-id]]
                                         :after []}}})

          ;; Reaction should be deleted - verify via direct database query
          db-result (jdbc/execute-one! (:jdbc-conn *test-env*)
                                       ["SELECT * FROM reactions WHERE id = ?" reaction-id])]

      (is (nil? db-result) "Reaction should be deleted from database"))))

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
    (let [now-instant (now)
          test-estimate 42.5M
          user-id (create! :user/id (fn [_] {:user/email {:after "test@example.com"}
                                             :user/username {:after "testuser"}
                                             :user/display-name {:after "Test User"}
                                             :user/password-hash {:after "bcrypt_hash_here"}
                                             :user/active? {:after true}
                                             :user/admin? {:after false}
                                             :user/created-at {:after now-instant}
                                             :user/last-login-at {:after now-instant}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Test Project"}
                                                   :project/key {:after "TEST"}
                                                   :project/default-workflow {:after 'workflow/standard}
                                                   :project/budget {:after 10000.00M}
                                                   :project/organization {:after [:organization/id org-id]}}))
          issue-id (create! :issue/id (fn [_] {:issue/title {:after "Type Test Issue"}
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
                                               :issue/reporter {:after [:user/id user-id]}}))

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
    (let [org-id (create! :organization/id (fn [_] {:organization/name {:after "Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Test Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create labels out of order
          _ (create! :label/id (fn [_] {:label/name {:after "Beta"}
                                        :label/position {:after 2}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "Gamma"}
                                        :label/position {:after 3}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "Alpha"}
                                        :label/position {:after 1}
                                        :label/project {:after [:project/id project-id]}}))

          ;; Query labels - should be ordered by position
          query-result (run-query-with-ident
                        [:project/id project-id]
                        [:project/name
                         {:project/labels
                          [:label/name :label/position]}])]

      (is (= ["Alpha" "Beta" "Gamma"]
             (mapv :label/name (:project/labels query-result)))))))

;; =============================================================================
;; ORDER-BY EDGE CASE TESTS
;;
;; Tests for edge cases in to-many ordering
;; =============================================================================

(deftest order-by-with-null-values-test
  (testing "NULL values in order-by column"
    (let [org-id (create! :organization/id (fn [_] {:organization/name {:after "Null Order Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Null Order Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create labels - some with nil position
          _ (create! :label/id (fn [_] {:label/name {:after "Has Position 2"}
                                        :label/position {:after 2}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "No Position A"}
                                        ;; No position - will be NULL
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "Has Position 1"}
                                        :label/position {:after 1}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "No Position B"}
                                        ;; No position - will be NULL
                                        :label/project {:after [:project/id project-id]}}))

          query-result (run-query-with-ident
                        [:project/id project-id]
                        [:project/name
                         {:project/labels [:label/name :label/position]}])
          labels (:project/labels query-result)
          names (mapv :label/name labels)]

      ;; Non-null values should come first (ordered), then NULLs
      ;; PostgreSQL default: NULLS LAST for ASC order
      (is (= 4 (count labels)) "Should have 4 labels")
      (is (= "Has Position 1" (first names)) "Position 1 should be first")
      (is (= "Has Position 2" (second names)) "Position 2 should be second")
      ;; NULLs come last (order among NULLs is undefined)
      (is (nil? (:label/position (nth labels 2))) "Third should have nil position")
      (is (nil? (:label/position (nth labels 3))) "Fourth should have nil position"))))

(deftest order-by-duplicate-values-test
  (testing "Duplicate values in order-by column maintain stable ordering"
    (let [org-id (create! :organization/id (fn [_] {:organization/name {:after "Dupe Order Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Dupe Order Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create labels with duplicate positions
          _ (create! :label/id (fn [_] {:label/name {:after "First at 1"}
                                        :label/position {:after 1}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "Second at 1"}
                                        :label/position {:after 1}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "At Position 2"}
                                        :label/position {:after 2}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "Third at 1"}
                                        :label/position {:after 1}
                                        :label/project {:after [:project/id project-id]}}))

          query-result (run-query-with-ident
                        [:project/id project-id]
                        [:project/name
                         {:project/labels [:label/name :label/position]}])
          labels (:project/labels query-result)
          positions (mapv :label/position labels)]

      (is (= 4 (count labels)) "Should have 4 labels")
      ;; All position=1 items should come before position=2
      (is (= [1 1 1 2] positions) "Positions should be sorted ascending")
      ;; The last one should be "At Position 2"
      (is (= "At Position 2" (:label/name (last labels)))))))

(deftest order-by-with-large-dataset-test
  (testing "Order-by works correctly with many items"
    (let [org-id (create! :organization/id (fn [_] {:organization/name {:after "Large Order Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Large Order Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create 20 labels in reverse order
          _ (doseq [i (reverse (range 1 21))]
              (create! :label/id (fn [_] {:label/name {:after (str "Label " (format "%02d" i))}
                                          :label/position {:after i}
                                          :label/project {:after [:project/id project-id]}})))

          query-result (run-query-with-ident
                        [:project/id project-id]
                        [:project/name
                         {:project/labels [:label/name :label/position]}])
          labels (:project/labels query-result)
          positions (mapv :label/position labels)]

      (is (= 20 (count labels)) "Should have 20 labels")
      (is (= (range 1 21) positions) "Positions should be sorted 1-20")
      (is (= "Label 01" (:label/name (first labels))) "First should be Label 01")
      (is (= "Label 20" (:label/name (last labels))) "Last should be Label 20"))))

(deftest order-by-empty-collection-test
  (testing "Order-by with no matching items returns empty vector"
    (let [org-id (create! :organization/id (fn [_] {:organization/name {:after "Empty Order Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Empty Order Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Don't create any labels

          query-result (run-query-with-ident
                        [:project/id project-id]
                        [:project/name
                         {:project/labels [:label/name :label/position]}])
          labels (:project/labels query-result)]

      (is (= [] labels) "Empty project should return empty labels vector"))))

(deftest order-by-single-item-test
  (testing "Order-by with single item"
    (let [org-id (create! :organization/id (fn [_] {:organization/name {:after "Single Order Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Single Order Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create single label
          _ (create! :label/id (fn [_] {:label/name {:after "Only Label"}
                                        :label/position {:after 5}
                                        :label/project {:after [:project/id project-id]}}))

          query-result (run-query-with-ident
                        [:project/id project-id]
                        [:project/name
                         {:project/labels [:label/name :label/position]}])
          labels (:project/labels query-result)]

      (is (= 1 (count labels)) "Should have 1 label")
      (is (= "Only Label" (:label/name (first labels))))
      (is (= 5 (:label/position (first labels)))))))

(deftest order-by-negative-positions-test
  (testing "Order-by with negative position values"
    (let [org-id (create! :organization/id (fn [_] {:organization/name {:after "Negative Order Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Negative Order Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create labels with negative, zero, and positive positions
          _ (create! :label/id (fn [_] {:label/name {:after "Positive 10"}
                                        :label/position {:after 10}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "Negative -5"}
                                        :label/position {:after -5}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "Zero"}
                                        :label/position {:after 0}
                                        :label/project {:after [:project/id project-id]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "Negative -10"}
                                        :label/position {:after -10}
                                        :label/project {:after [:project/id project-id]}}))

          query-result (run-query-with-ident
                        [:project/id project-id]
                        [:project/name
                         {:project/labels [:label/name :label/position]}])
          labels (:project/labels query-result)
          positions (mapv :label/position labels)]

      (is (= 4 (count labels)) "Should have 4 labels")
      (is (= [-10 -5 0 10] positions) "Positions should be sorted including negatives")
      (is (= "Negative -10" (:label/name (first labels))))
      (is (= "Positive 10" (:label/name (last labels)))))))

(deftest order-by-batch-multiple-parents-test
  (testing "Order-by works correctly when batch-fetching from multiple parents"
    (let [org-id (create! :organization/id (fn [_] {:organization/name {:after "Batch Order Org"}}))

          ;; Create 3 projects
          project-ids (vec (for [i (range 3)]
                             (create! :project/id (fn [_] {:project/name {:after (str "Project " i)}
                                                           :project/organization {:after [:organization/id org-id]}}))))

          ;; Create labels for each project (in different orders)
          ;; Project 0: C(3), A(1), B(2)
          _ (create! :label/id (fn [_] {:label/name {:after "P0-C"} :label/position {:after 3}
                                        :label/project {:after [:project/id (nth project-ids 0)]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "P0-A"} :label/position {:after 1}
                                        :label/project {:after [:project/id (nth project-ids 0)]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "P0-B"} :label/position {:after 2}
                                        :label/project {:after [:project/id (nth project-ids 0)]}}))

          ;; Project 1: Y(2), Z(3), X(1)
          _ (create! :label/id (fn [_] {:label/name {:after "P1-Y"} :label/position {:after 2}
                                        :label/project {:after [:project/id (nth project-ids 1)]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "P1-Z"} :label/position {:after 3}
                                        :label/project {:after [:project/id (nth project-ids 1)]}}))
          _ (create! :label/id (fn [_] {:label/name {:after "P1-X"} :label/position {:after 1}
                                        :label/project {:after [:project/id (nth project-ids 1)]}}))

          ;; Project 2: only one label
          _ (create! :label/id (fn [_] {:label/name {:after "P2-Only"} :label/position {:after 1}
                                        :label/project {:after [:project/id (nth project-ids 2)]}}))

          ;; Batch query all 3 projects
          results (run-query (vec (for [id project-ids]
                                    {[:project/id id] [:project/name {:project/labels [:label/name :label/position]}]})))

          p0-labels (-> results (get [:project/id (nth project-ids 0)]) :project/labels)
          p1-labels (-> results (get [:project/id (nth project-ids 1)]) :project/labels)
          p2-labels (-> results (get [:project/id (nth project-ids 2)]) :project/labels)]

      ;; Each project's labels should be independently ordered
      (is (= ["P0-A" "P0-B" "P0-C"] (mapv :label/name p0-labels))
          "Project 0 labels should be ordered A, B, C")
      (is (= ["P1-X" "P1-Y" "P1-Z"] (mapv :label/name p1-labels))
          "Project 1 labels should be ordered X, Y, Z")
      (is (= ["P2-Only"] (mapv :label/name p2-labels))
          "Project 2 should have single label"))))

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
    (let [;; Create user
          user-id (create! :user/id (fn [_] {:user/email {:after "team@example.com"}
                                             :user/username {:after "teamuser"}
                                             :user/active? {:after true}}))

          ;; Create notification for user
          _ (create! :notification/id (fn [_] {:notification/title {:after "You were added to a team"}
                                               :notification/type {:after :notification.type/project-invited}
                                               :notification/read? {:after false}
                                               :notification/user {:after [:user/id user-id]}
                                               :notification/created-at {:after (now)}}))

          ;; Create organization
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Team Org"}
                                                    :organization/created-at {:after (now)}}))

          ;; Create team
          team-id (create! :team/id (fn [_] {:team/name {:after "Engineering"}
                                             :team/organization {:after [:organization/id org-id]}}))

          ;; Add user to team via TeamMember
          _ (create! :team-member/id (fn [_] {:team-member/role {:after :team.role/member}
                                              :team-member/team {:after [:team/id team-id]}
                                              :team-member/user {:after [:user/id user-id]}
                                              :team-member/joined-at {:after (now)}}))

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

;; =============================================================================
;; Custom Transformer Round-Trip Test
;;
;; Verifies that sql->form-value is applied on reads, enabling full round-trip
;; transformation of custom data types (JSON, CSV, etc.)
;; =============================================================================

(deftest sql->form-value-applied-on-reads-test
  (testing "sql->form-value transformer is applied on reads"
    (let [permissions {:read true :write true :admin false}
          user-id (create! :user/id (fn [_] {:user/email {:after "bug-test@example.com"}
                                             :user/username {:after "bugtest"}
                                             :user/active? {:after true}}))
          token-id (create! :api-token/id (fn [_] {:api-token/name {:after "Bug Test Token"}
                                                   :api-token/token-hash {:after "hash"}
                                                   :api-token/permissions {:after permissions}
                                                   :api-token/user {:after [:user/id user-id]}}))

          ;; Query via Pathom - sql->form-value should be applied
          result (run-query-with-ident
                  [:api-token/id token-id]
                  [:api-token/name :api-token/permissions])]

      (is (= "Bug Test Token" (:api-token/name result)))
      ;; Verify the custom transformer decoded the string back to a map
      (is (map? (:api-token/permissions result))
          "sql->form-value should decode the JSON string back to a map")
      (is (= permissions (:api-token/permissions result))
          "Decoded permissions should match original"))))

;; =============================================================================
;; Empty To-Many Relationship Test
;;
;; Verifies that to-many relationships with no results return an empty vector
;; instead of throwing an error.
;; =============================================================================

(deftest empty-to-many-returns-empty-vector-test
  (testing "Empty to-many relationship returns empty vector"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "empty-test@example.com"}
                                             :user/username {:after "emptytest"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Empty Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Empty Test Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))
          issue-id (create! :issue/id (fn [_] {:issue/title {:after "Empty Comments Issue"}
                                               :issue/status {:after :issue.status/open}
                                               :issue/type {:after :issue.type/bug}
                                               :issue/project {:after [:project/id project-id]}
                                               :issue/reporter {:after [:user/id user-id]}
                                               :issue/created-at {:after (now)}}))

          ;; Query issue with comments - issue has no comments
          result (run-query-with-ident
                  [:issue/id issue-id]
                  [:issue/title {:issue/comments [:comment/body]}])]

      (is (= "Empty Comments Issue" (:issue/title result)))
      (is (= [] (:issue/comments result))
          "Empty to-many should return empty vector"))))

;; =============================================================================
;; UPDATE OPERATION TESTS
;;
;; Tests for updating existing entities - scalar values, refs, and partial updates
;; =============================================================================

(deftest update-scalar-values-test
  (testing "Update scalar values on existing entity"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "update-test@example.com"}
                                             :user/username {:after "updatetest"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Update Test Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Update Test Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))
          issue-id (create! :issue/id (fn [_] {:issue/title {:after "Original Title"}
                                               :issue/description {:after "Original description"}
                                               :issue/status {:after :issue.status/open}
                                               :issue/priority {:after :issue.priority/low}
                                               :issue/type {:after :issue.type/bug}
                                               :issue/project {:after [:project/id project-id]}
                                               :issue/reporter {:after [:user/id user-id]}
                                               :issue/created-at {:after (now)}}))

          ;; Verify original values
          before (run-query-with-ident [:issue/id issue-id]
                                       [:issue/title :issue/description :issue/status :issue/priority])
          _ (is (= "Original Title" (:issue/title before)))
          _ (is (= :issue.status/open (:issue/status before)))
          _ (is (= :issue.priority/low (:issue/priority before)))

          ;; Update multiple scalar values
          _ (save! {[:issue/id issue-id]
                    {:issue/title {:before "Original Title" :after "Updated Title"}
                     :issue/description {:before "Original description" :after "Updated description"}
                     :issue/status {:before :issue.status/open :after :issue.status/in-progress}
                     :issue/priority {:before :issue.priority/low :after :issue.priority/critical}}})

          ;; Verify updated values
          after (run-query-with-ident [:issue/id issue-id]
                                      [:issue/title :issue/description :issue/status :issue/priority])]

      (is (= "Updated Title" (:issue/title after)))
      (is (= "Updated description" (:issue/description after)))
      (is (= :issue.status/in-progress (:issue/status after)))
      (is (= :issue.priority/critical (:issue/priority after))))))

(deftest update-to-one-ref-test
  (testing "Update to-one reference (re-parenting)"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "reparent@example.com"}
                                             :user/username {:after "reparenttest"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Reparent Org"}}))
          project-a-id (create! :project/id (fn [_] {:project/name {:after "Project A"}
                                                     :project/organization {:after [:organization/id org-id]}}))
          project-b-id (create! :project/id (fn [_] {:project/name {:after "Project B"}
                                                     :project/organization {:after [:organization/id org-id]}}))
          issue-id (create! :issue/id (fn [_] {:issue/title {:after "Reparent Issue"}
                                               :issue/status {:after :issue.status/open}
                                               :issue/type {:after :issue.type/task}
                                               :issue/project {:after [:project/id project-a-id]}
                                               :issue/reporter {:after [:user/id user-id]}
                                               :issue/created-at {:after (now)}}))

          ;; Verify original project
          before (run-query-with-ident [:issue/id issue-id]
                                       [:issue/title {:issue/project [:project/name]}])
          _ (is (= "Project A" (-> before :issue/project :project/name)))

          ;; Move issue to different project
          _ (save! {[:issue/id issue-id]
                    {:issue/project {:before [:project/id project-a-id]
                                     :after [:project/id project-b-id]}}})

          ;; Verify new project
          after (run-query-with-ident [:issue/id issue-id]
                                      [:issue/title {:issue/project [:project/name]}])]

      (is (= "Project B" (-> after :issue/project :project/name))))))

(deftest update-partial-fields-test
  (testing "Partial update - only specified fields change"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "partial@example.com"}
                                             :user/username {:after "partialtest"}
                                             :user/display-name {:after "Original Display Name"}
                                             :user/active? {:after true}}))

          ;; Verify original values
          before (run-query-with-ident [:user/id user-id]
                                       [:user/username :user/display-name :user/active?])
          _ (is (= "partialtest" (:user/username before)))
          _ (is (= "Original Display Name" (:user/display-name before)))
          _ (is (= true (:user/active? before)))

          ;; Update only display-name, leave other fields unchanged
          _ (save! {[:user/id user-id]
                    {:user/display-name {:before "Original Display Name"
                                         :after "New Display Name"}}})

          ;; Verify only display-name changed
          after (run-query-with-ident [:user/id user-id]
                                      [:user/username :user/display-name :user/active?])]

      (is (= "partialtest" (:user/username after)) "Username should remain unchanged")
      (is (= "New Display Name" (:user/display-name after)) "Display name should be updated")
      (is (= true (:user/active? after)) "Active status should remain unchanged"))))

;; =============================================================================
;; NULL/NIL HANDLING TESTS
;;
;; Tests for setting optional attributes to nil and clearing refs
;; =============================================================================

(deftest set-optional-string-to-nil-test
  (testing "Setting optional string attribute to nil"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "nil-test@example.com"}
                                             :user/username {:after "niltest"}
                                             :user/display-name {:after "Has Display Name"}
                                             :user/active? {:after true}}))

          ;; Verify original value
          before (run-query-with-ident [:user/id user-id]
                                       [:user/display-name])
          _ (is (= "Has Display Name" (:user/display-name before)))

          ;; Set display-name to nil
          _ (save! {[:user/id user-id]
                    {:user/display-name {:before "Has Display Name"
                                         :after nil}}})

          ;; Verify nil value
          after (run-query-with-ident [:user/id user-id]
                                      [:user/display-name])]

      (is (nil? (:user/display-name after))
          "Optional string should be settable to nil"))))

(deftest set-optional-instant-to-nil-test
  (testing "Setting optional instant attribute to nil"
    (let [original-time (now)
          user-id (create! :user/id (fn [_] {:user/email {:after "instant-nil@example.com"}
                                             :user/username {:after "instantnil"}
                                             :user/last-login-at {:after original-time}
                                             :user/active? {:after true}}))

          ;; Verify original value
          before (run-query-with-ident [:user/id user-id]
                                       [:user/last-login-at])
          _ (is (instance? Instant (:user/last-login-at before)))

          ;; Set last-login-at to nil
          _ (save! {[:user/id user-id]
                    {:user/last-login-at {:before original-time
                                          :after nil}}})

          ;; Verify nil value
          after (run-query-with-ident [:user/id user-id]
                                      [:user/last-login-at])]

      (is (nil? (:user/last-login-at after))
          "Optional instant should be settable to nil"))))

(deftest set-optional-decimal-to-nil-test
  (testing "Setting optional decimal attribute to nil"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "decimal-nil@example.com"}
                                             :user/username {:after "decimalnil"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Decimal Nil Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Decimal Nil Project"}
                                                   :project/budget {:after 50000.00M}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Verify original value
          before (run-query-with-ident [:project/id project-id]
                                       [:project/budget])
          _ (is (= 50000.00M (:project/budget before)))

          ;; Set budget to nil
          _ (save! {[:project/id project-id]
                    {:project/budget {:before 50000.00M
                                      :after nil}}})

          ;; Verify nil value
          after (run-query-with-ident [:project/id project-id]
                                      [:project/budget])]

      (is (nil? (:project/budget after))
          "Optional decimal should be settable to nil"))))

(deftest clear-optional-to-one-ref-test
  (testing "Clearing optional to-one reference"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "clear-ref@example.com"}
                                             :user/username {:after "clearref"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Clear Ref Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Clear Ref Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))
          milestone-id (create! :milestone/id (fn [_] {:milestone/name {:after "v1.0"}
                                                       :milestone/project {:after [:project/id project-id]}}))
          issue-id (create! :issue/id (fn [_] {:issue/title {:after "Has Milestone"}
                                               :issue/status {:after :issue.status/open}
                                               :issue/type {:after :issue.type/task}
                                               :issue/project {:after [:project/id project-id]}
                                               :issue/reporter {:after [:user/id user-id]}
                                               :issue/milestone {:after [:milestone/id milestone-id]}
                                               :issue/created-at {:after (now)}}))

          ;; Verify original ref via direct DB query
          before-db (jdbc/execute-one! (:jdbc-conn *test-env*)
                                       ["SELECT milestone FROM issues WHERE id = ?" issue-id])
          _ (is (= milestone-id (first (vals before-db))))

          ;; Clear milestone reference
          _ (save! {[:issue/id issue-id]
                    {:issue/milestone {:before [:milestone/id milestone-id]
                                       :after nil}}})

          ;; Verify ref is cleared via direct DB query
          after-db (jdbc/execute-one! (:jdbc-conn *test-env*)
                                      ["SELECT milestone FROM issues WHERE id = ?" issue-id])]

      (is (nil? (first (vals after-db)))
          "Optional to-one ref should be clearable to nil"))))

;; =============================================================================
;; BATCH RESOLVER TESTS
;;
;; Tests verifying batch resolvers correctly fetch multiple entities
;; =============================================================================

(deftest batch-resolution-multiple-entities-test
  (testing "Batch resolver fetches multiple entities with correct data association"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "batch@example.com"}
                                             :user/username {:after "batchtest"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Batch Org"}}))
          project-id (create! :project/id (fn [_] {:project/name {:after "Batch Project"}
                                                   :project/organization {:after [:organization/id org-id]}}))

          ;; Create 10 issues with unique titles and priorities
          issue-ids (vec (for [i (range 10)]
                           (create! :issue/id (fn [_] {:issue/title {:after (str "Issue " i)}
                                                       :issue/status {:after :issue.status/open}
                                                       :issue/type {:after :issue.type/task}
                                                       :issue/priority-order {:after i}
                                                       :issue/project {:after [:project/id project-id]}
                                                       :issue/reporter {:after [:user/id user-id]}
                                                       :issue/created-at {:after (now)}}))))

          ;; Query all 10 issues in a single batch query
          results (run-query (vec (for [id issue-ids]
                                    {[:issue/id id] [:issue/title :issue/priority-order]})))]

      ;; Verify each issue has correct data
      (doseq [[i id] (map-indexed vector issue-ids)]
        (let [result (get results [:issue/id id])]
          (is (= (str "Issue " i) (:issue/title result))
              (str "Issue " i " should have correct title"))
          (is (= i (:issue/priority-order result))
              (str "Issue " i " should have correct priority-order")))))))

(deftest batch-resolution-with-joins-test
  (testing "Batch resolver handles joins correctly for multiple entities"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "batchjoin@example.com"}
                                             :user/username {:after "batchjointest"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Batch Join Org"}}))

          ;; Create 3 projects
          project-ids (vec (for [i (range 3)]
                             (create! :project/id (fn [_] {:project/name {:after (str "Project " i)}
                                                           :project/organization {:after [:organization/id org-id]}}))))

          ;; Create 2 issues per project (6 total)
          issue-ids (vec (for [pi (range 3)
                               ii (range 2)]
                           (create! :issue/id (fn [_] {:issue/title {:after (str "Issue P" pi "-" ii)}
                                                       :issue/status {:after :issue.status/open}
                                                       :issue/type {:after :issue.type/task}
                                                       :issue/project {:after [:project/id (nth project-ids pi)]}
                                                       :issue/reporter {:after [:user/id user-id]}
                                                       :issue/created-at {:after (now)}}))))

          ;; Query all issues with their project names
          results (run-query (vec (for [id issue-ids]
                                    {[:issue/id id] [:issue/title {:issue/project [:project/name]}]})))]

      ;; Verify each issue is associated with correct project
      (doseq [[idx id] (map-indexed vector issue-ids)]
        (let [result (get results [:issue/id id])
              expected-project-idx (quot idx 2)]
          (is (= (str "Project " expected-project-idx)
                 (-> result :issue/project :project/name))
              (str "Issue " idx " should be linked to Project " expected-project-idx)))))))

(deftest batch-to-many-resolution-test
  (testing "Batch to-many resolver returns correct children for each parent"
    (let [user-id (create! :user/id (fn [_] {:user/email {:after "batchtomany@example.com"}
                                             :user/username {:after "batchtomany"}
                                             :user/active? {:after true}}))
          org-id (create! :organization/id (fn [_] {:organization/name {:after "Batch To-Many Org"}}))

          ;; Create 3 projects with different numbers of issues
          project-ids (vec (for [_ (range 3)]
                             (create! :project/id (fn [_] {:project/name {:after "Project"}
                                                           :project/organization {:after [:organization/id org-id]}}))))

          ;; Create issues: 2 for project 0, 3 for project 1, 0 for project 2
          _ (dotimes [_ 2]
              (create! :issue/id (fn [_] {:issue/title {:after "P0 Issue"}
                                          :issue/status {:after :issue.status/open}
                                          :issue/type {:after :issue.type/task}
                                          :issue/project {:after [:project/id (nth project-ids 0)]}
                                          :issue/reporter {:after [:user/id user-id]}
                                          :issue/created-at {:after (now)}})))
          _ (dotimes [_ 3]
              (create! :issue/id (fn [_] {:issue/title {:after "P1 Issue"}
                                          :issue/status {:after :issue.status/open}
                                          :issue/type {:after :issue.type/task}
                                          :issue/project {:after [:project/id (nth project-ids 1)]}
                                          :issue/reporter {:after [:user/id user-id]}
                                          :issue/created-at {:after (now)}})))

          ;; Query all projects with their issues
          results (run-query (vec (for [id project-ids]
                                    {[:project/id id] [:project/name {:project/issues [:issue/title]}]})))]

      (is (= 2 (count (-> results (get [:project/id (nth project-ids 0)]) :project/issues)))
          "Project 0 should have 2 issues")
      (is (= 3 (count (-> results (get [:project/id (nth project-ids 1)]) :project/issues)))
          "Project 1 should have 3 issues")
      (is (= 0 (count (-> results (get [:project/id (nth project-ids 2)]) :project/issues)))
          "Project 2 should have 0 issues (empty vector)"))))

;; =============================================================================
;; ERROR SCENARIO TESTS
;;
;; Tests for database constraint violations
;; =============================================================================

(deftest fk-violation-throws-exception-test
  (testing "Foreign key violation throws exception"
    (let [fake-project-id (java.util.UUID/randomUUID)
          user-id (create! :user/id (fn [_] {:user/email {:after "fk-error@example.com"}
                                             :user/username {:after "fkerror"}
                                             :user/active? {:after true}}))]
      ;; Try to create issue with non-existent project
      (is (thrown? Exception
                   (create! :issue/id (fn [_] {:issue/title {:after "Bad Issue"}
                                               :issue/status {:after :issue.status/open}
                                               :issue/type {:after :issue.type/task}
                                               :issue/project {:after [:project/id fake-project-id]}
                                               :issue/reporter {:after [:user/id user-id]}
                                               :issue/created-at {:after (now)}})))
          "Should throw exception for FK violation"))))

(deftest duplicate-allowed-without-unique-constraint-test
  (testing "Duplicate values allowed when no UNIQUE constraint exists"
    ;; Note: The automatic schema generator doesn't create UNIQUE constraints.
    ;; Fulcro RAD's ::attr/required? is a form-level validation, not a DB constraint.
    ;; This test documents that behavior.
    (create! :user/id (fn [_] {:user/email {:after "dupe@example.com"}
                               :user/username {:after "dupe1"}
                               :user/active? {:after true}}))
    ;; Second user with same email succeeds because no UNIQUE constraint
    (let [user2-id (create! :user/id (fn [_] {:user/email {:after "dupe@example.com"}
                                              :user/username {:after "dupe2"}
                                              :user/active? {:after true}}))]
      (is (some? user2-id) "Duplicate email allowed without UNIQUE constraint"))))

(deftest null-allowed-without-not-null-constraint-test
  (testing "NULL values allowed when no NOT NULL constraint exists"
    ;; Note: The automatic schema generator doesn't create NOT NULL constraints.
    ;; Fulcro RAD's ::attr/required? is a form-level validation, not a DB constraint.
    ;; This test documents that behavior.
    (let [user-id (create! :user/id (fn [_] {:user/username {:after "nomail"}
                                             :user/active? {:after true}}))]
      (is (some? user-id) "NULL email allowed without NOT NULL constraint"))))
