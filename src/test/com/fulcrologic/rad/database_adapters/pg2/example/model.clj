(ns com.fulcrologic.rad.database-adapters.pg2.example.model
  "Comprehensive issue tracker model for testing fulcro-rad-pg2.

   This model exercises ALL features of the library:

   Types covered:
   - :uuid      - Most entity IDs
   - :long      - Issue ID (sequence-based, like GitHub issue numbers)
   - :int       - Priority, position, vote counts
   - :string    - Names, descriptions, titles
   - :password  - User password hash, API tokens
   - :boolean   - Flags (active, archived, closed, public)
   - :decimal   - Time estimates, budgets, hours logged
   - :instant   - Timestamps (created, updated, due dates)
   - :enum      - Status, priority, role, reaction type
   - :keyword   - Color codes, categories
   - :symbol    - Workflow states (for state machines)

   Options covered:
   - ::pg2/table          - Custom table names
   - ::pg2/column-name    - Custom column names
   - ::pg2/max-length     - String length limits
   - ::pg2/fk-attr        - Reverse reference FK ownership
   - ::pg2/delete-orphan? - Cascade delete on reference removal
   - ::pg2/order-by       - Ordered collections
   - ::pg2/form->sql-value - Custom write transformers
   - ::pg2/sql->form-value - Custom read transformers

   Relationship patterns:
   - To-one forward (entity owns FK)
   - To-one reverse (target owns FK)
   - To-many reverse (target owns FK)
   - Many-to-many via join tables (3 examples)
   - Self-referential (Issue parent/children)
   - Owned components with delete-orphan?

   Query depth: 5+ levels
   - Organization → Project → Issue → Comment → Reaction
   - Organization → Team → TeamMember → User → Notification

   Entity hierarchy:
   ┌─────────────────────────────────────────────────────────────────┐
   │ Organization                                                     │
   │ ├── Team ──────────────────┐                                    │
   │ │   └── TeamMember ←───────┼──→ User                            │
   │ │                          │    ├── Notification                │
   │ └── Project                │    └── ApiToken                    │
   │     ├── ProjectMember ←────┘                                    │
   │     ├── Label                                                   │
   │     ├── Milestone                                               │
   │     │   └── Issue (self-ref: parent/children)                  │
   │     │       ├── Comment                                         │
   │     │       │   └── Reaction                                    │
   │     │       ├── Attachment (delete-orphan)                      │
   │     │       ├── TimeEntry                                       │
   │     │       ├── IssueLabel ←──→ Label (M:M)                    │
   │     │       ├── IssueWatcher ←──→ User (M:M)                   │
   │     │       └── IssueAssignee ←──→ User (M:M)                  │
   │     └── Webhook                                                 │
   └─────────────────────────────────────────────────────────────────┘"
  (:require
   [clojure.string :as str]
   [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
   [com.fulcrologic.rad.database-adapters.pg2 :as pg2]))

;; =============================================================================
;; Custom Value Transformers
;; =============================================================================

(defn tags->csv
  "Convert vector of tags to comma-separated string for storage."
  [tags]
  (when (seq tags)
    (str/join "," (map name tags))))

(defn csv->tags
  "Convert comma-separated string back to vector of keywords."
  [csv]
  (when (and csv (not (str/blank? csv)))
    (mapv keyword (str/split csv #","))))

(defn json-encode
  "Encode Clojure data as JSON string."
  [data]
  (when data
    ;; Using pr-str for simplicity; in production use jsonista
    (pr-str data)))

(defn json-decode
  "Decode JSON string to Clojure data."
  [json-str]
  (when json-str
    (read-string json-str)))

;; =============================================================================
;; Organization - Top-level entity
;; =============================================================================

(defattr org-id :organization/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "organizations"})

(defattr org-name :organization/name :string
  {::attr/schema :tracker
   ::attr/identities #{:organization/id}
   ::attr/required? true
   ::pg2/max-length 100})

(defattr org-slug :organization/slug :string
  {::attr/schema :tracker
   ::attr/identities #{:organization/id}
   ::pg2/max-length 50
   ::pg2/column-name "url_slug"})

(defattr org-description :organization/description :string
  {::attr/schema :tracker
   ::attr/identities #{:organization/id}
   ::pg2/max-length 2000})

(defattr org-public? :organization/public? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:organization/id}
   ::pg2/column-name "is_public"})

(defattr org-created-at :organization/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:organization/id}
   ::pg2/column-name "created_at"})

;; To-many: Organization has many projects
(defattr org-projects :organization/projects :ref
  {::attr/cardinality :many
   ::attr/target :project/id
   ::attr/schema :tracker
   ::attr/identities #{:organization/id}
   ::pg2/fk-attr :project/organization})

;; To-many: Organization has many teams
(defattr org-teams :organization/teams :ref
  {::attr/cardinality :many
   ::attr/target :team/id
   ::attr/schema :tracker
   ::attr/identities #{:organization/id}
   ::pg2/fk-attr :team/organization})

(def organization-attributes
  [org-id org-name org-slug org-description org-public? org-created-at
   org-projects org-teams])

;; =============================================================================
;; Team - Groups users within an organization
;; =============================================================================

(defattr team-id :team/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "teams"})

(defattr team-name :team/name :string
  {::attr/schema :tracker
   ::attr/identities #{:team/id}
   ::attr/required? true
   ::pg2/max-length 100})

(defattr team-description :team/description :string
  {::attr/schema :tracker
   ::attr/identities #{:team/id}
   ::pg2/max-length 500})

;; To-one: Team belongs to organization
(defattr team-organization :team/organization :ref
  {::attr/cardinality :one
   ::attr/target :organization/id
   ::attr/schema :tracker
   ::attr/identities #{:team/id}})

;; To-many: Team has many members (via join table)
(defattr team-members :team/members :ref
  {::attr/cardinality :many
   ::attr/target :team-member/id
   ::attr/schema :tracker
   ::attr/identities #{:team/id}
   ::pg2/fk-attr :team-member/team})

(def team-attributes
  [team-id team-name team-description team-organization team-members])

;; =============================================================================
;; User - System users
;; =============================================================================

(defattr user-id :user/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "users"})

(defattr user-email :user/email :string
  {::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::attr/required? true
   ::pg2/max-length 255})

(defattr user-username :user/username :string
  {::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::attr/required? true
   ::pg2/max-length 50})

(defattr user-display-name :user/display-name :string
  {::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::pg2/max-length 100
   ::pg2/column-name "display_name"})

(defattr user-password-hash :user/password-hash :password
  {::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::pg2/column-name "password_hash"})

(defattr user-active? :user/active? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::pg2/column-name "is_active"})

(defattr user-admin? :user/admin? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::pg2/column-name "is_admin"})

(defattr user-created-at :user/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::pg2/column-name "created_at"})

(defattr user-last-login-at :user/last-login-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::pg2/column-name "last_login_at"})

;; To-many: User has many notifications
(defattr user-notifications :user/notifications :ref
  {::attr/cardinality :many
   ::attr/target :notification/id
   ::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::pg2/fk-attr :notification/user
   ::pg2/order-by :notification/created-at})

;; To-many: User has many API tokens (owned, delete-orphan)
(defattr user-api-tokens :user/api-tokens :ref
  {::attr/cardinality :many
   ::attr/target :api-token/id
   ::attr/schema :tracker
   ::attr/identities #{:user/id}
   ::pg2/fk-attr :api-token/user
   ::pg2/delete-orphan? true})

(def user-attributes
  [user-id user-email user-username user-display-name user-password-hash
   user-active? user-admin? user-created-at user-last-login-at
   user-notifications user-api-tokens])

;; =============================================================================
;; ApiToken - User's API access tokens (owned component)
;; =============================================================================

(defattr api-token-id :api-token/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "api_tokens"})

(defattr api-token-name :api-token/name :string
  {::attr/schema :tracker
   ::attr/identities #{:api-token/id}
   ::pg2/max-length 100})

;; Stored encrypted/hashed
(defattr api-token-token-hash :api-token/token-hash :password
  {::attr/schema :tracker
   ::attr/identities #{:api-token/id}
   ::pg2/column-name "token_hash"})

;; Permissions stored as JSON
(defattr api-token-permissions :api-token/permissions :string
  {::attr/schema :tracker
   ::attr/identities #{:api-token/id}
   ::pg2/max-length 1000
   ::pg2/form->sql-value json-encode
   ::pg2/sql->form-value json-decode})

(defattr api-token-expires-at :api-token/expires-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:api-token/id}
   ::pg2/column-name "expires_at"})

(defattr api-token-last-used-at :api-token/last-used-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:api-token/id}
   ::pg2/column-name "last_used_at"})

;; To-one: Token belongs to user
(defattr api-token-user :api-token/user :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:api-token/id}})

(def api-token-attributes
  [api-token-id api-token-name api-token-token-hash api-token-permissions
   api-token-expires-at api-token-last-used-at api-token-user])

;; =============================================================================
;; Notification - User notifications
;; =============================================================================

(defattr notification-id :notification/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "notifications"})

(defattr notification-title :notification/title :string
  {::attr/schema :tracker
   ::attr/identities #{:notification/id}
   ::pg2/max-length 200})

(defattr notification-body :notification/body :string
  {::attr/schema :tracker
   ::attr/identities #{:notification/id}
   ::pg2/max-length 2000})

(def notification-types #{:notification.type/issue-assigned
                          :notification.type/issue-mentioned
                          :notification.type/comment-added
                          :notification.type/issue-closed
                          :notification.type/project-invited})

(defattr notification-type :notification/type :enum
  {::attr/schema :tracker
   ::attr/identities #{:notification/id}
   ::attr/enumerated-values notification-types
   ::pg2/column-name "notification_type"})

(defattr notification-read? :notification/read? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:notification/id}
   ::pg2/column-name "is_read"})

(defattr notification-created-at :notification/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:notification/id}
   ::pg2/column-name "created_at"})

;; To-one: Notification belongs to user
(defattr notification-user :notification/user :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:notification/id}})

;; To-one: Optional link to related issue
(defattr notification-issue :notification/issue :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:notification/id}})

(def notification-attributes
  [notification-id notification-title notification-body notification-type
   notification-read? notification-created-at notification-user notification-issue])

;; =============================================================================
;; TeamMember - Join table: User ↔ Team (many-to-many with role)
;; =============================================================================

(defattr team-member-id :team-member/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "team_members"})

(def team-roles #{:team.role/admin :team.role/member :team.role/viewer})

(defattr team-member-role :team-member/role :enum
  {::attr/schema :tracker
   ::attr/identities #{:team-member/id}
   ::attr/enumerated-values team-roles})

(defattr team-member-joined-at :team-member/joined-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:team-member/id}
   ::pg2/column-name "joined_at"})

;; To-one: TeamMember → Team
(defattr team-member-team :team-member/team :ref
  {::attr/cardinality :one
   ::attr/target :team/id
   ::attr/schema :tracker
   ::attr/identities #{:team-member/id}})

;; To-one: TeamMember → User
(defattr team-member-user :team-member/user :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:team-member/id}})

(def team-member-attributes
  [team-member-id team-member-role team-member-joined-at
   team-member-team team-member-user])

;; =============================================================================
;; Project - Container for issues
;; =============================================================================

(defattr project-id :project/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "projects"})

(defattr project-name :project/name :string
  {::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::attr/required? true
   ::pg2/max-length 100})

(defattr project-key :project/key :string
  {::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/max-length 10
   ::pg2/column-name "project_key"})

(defattr project-description :project/description :string
  {::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/max-length 2000})

(defattr project-public? :project/public? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/column-name "is_public"})

(defattr project-archived? :project/archived? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/column-name "is_archived"})

(defattr project-created-at :project/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/column-name "created_at"})

;; Default workflow state (symbol type)
(defattr project-default-workflow :project/default-workflow :symbol
  {::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/column-name "default_workflow"})

;; Budget for the project
(defattr project-budget :project/budget :decimal
  {::attr/schema :tracker
   ::attr/identities #{:project/id}})

;; To-one: Project belongs to organization
(defattr project-organization :project/organization :ref
  {::attr/cardinality :one
   ::attr/target :organization/id
   ::attr/schema :tracker
   ::attr/identities #{:project/id}})

;; To-one: Project lead (user)
(defattr project-lead :project/lead :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:project/id}})

;; To-many: Project has many milestones
(defattr project-milestones :project/milestones :ref
  {::attr/cardinality :many
   ::attr/target :milestone/id
   ::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/fk-attr :milestone/project
   ::pg2/order-by :milestone/due-date})

;; To-many: Project has many issues
(defattr project-issues :project/issues :ref
  {::attr/cardinality :many
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/fk-attr :issue/project})

;; To-many: Project has many labels
(defattr project-labels :project/labels :ref
  {::attr/cardinality :many
   ::attr/target :label/id
   ::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/fk-attr :label/project
   ::pg2/order-by :label/position})

;; To-many: Project has many members (via join table)
(defattr project-members :project/members :ref
  {::attr/cardinality :many
   ::attr/target :project-member/id
   ::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/fk-attr :project-member/project})

;; To-many: Project has many webhooks (owned)
(defattr project-webhooks :project/webhooks :ref
  {::attr/cardinality :many
   ::attr/target :webhook/id
   ::attr/schema :tracker
   ::attr/identities #{:project/id}
   ::pg2/fk-attr :webhook/project
   ::pg2/delete-orphan? true})

(def project-attributes
  [project-id project-name project-key project-description project-public?
   project-archived? project-created-at project-default-workflow project-budget
   project-organization project-lead project-milestones project-issues
   project-labels project-members project-webhooks])

;; =============================================================================
;; ProjectMember - Join table: User ↔ Project (many-to-many with role)
;; =============================================================================

(defattr project-member-id :project-member/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "project_members"})

(def project-roles #{:project.role/admin :project.role/developer
                     :project.role/reporter :project.role/viewer})

(defattr project-member-role :project-member/role :enum
  {::attr/schema :tracker
   ::attr/identities #{:project-member/id}
   ::attr/enumerated-values project-roles})

(defattr project-member-joined-at :project-member/joined-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:project-member/id}
   ::pg2/column-name "joined_at"})

;; To-one: ProjectMember → Project
(defattr project-member-project :project-member/project :ref
  {::attr/cardinality :one
   ::attr/target :project/id
   ::attr/schema :tracker
   ::attr/identities #{:project-member/id}})

;; To-one: ProjectMember → User
(defattr project-member-user :project-member/user :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:project-member/id}})

(def project-member-attributes
  [project-member-id project-member-role project-member-joined-at
   project-member-project project-member-user])

;; =============================================================================
;; Webhook - Project webhooks (owned component)
;; =============================================================================

(defattr webhook-id :webhook/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "webhooks"})

(defattr webhook-name :webhook/name :string
  {::attr/schema :tracker
   ::attr/identities #{:webhook/id}
   ::pg2/max-length 100})

(defattr webhook-url :webhook/url :string
  {::attr/schema :tracker
   ::attr/identities #{:webhook/id}
   ::pg2/max-length 500})

(defattr webhook-secret :webhook/secret :password
  {::attr/schema :tracker
   ::attr/identities #{:webhook/id}})

(defattr webhook-active? :webhook/active? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:webhook/id}
   ::pg2/column-name "is_active"})

;; Events to trigger on (stored as CSV keywords)
(defattr webhook-events :webhook/events :string
  {::attr/schema :tracker
   ::attr/identities #{:webhook/id}
   ::pg2/max-length 500
   ::pg2/form->sql-value tags->csv
   ::pg2/sql->form-value csv->tags})

;; To-one: Webhook belongs to project
(defattr webhook-project :webhook/project :ref
  {::attr/cardinality :one
   ::attr/target :project/id
   ::attr/schema :tracker
   ::attr/identities #{:webhook/id}})

(def webhook-attributes
  [webhook-id webhook-name webhook-url webhook-secret webhook-active?
   webhook-events webhook-project])

;; =============================================================================
;; Label - Issue labels within a project
;; =============================================================================

(defattr label-id :label/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "labels"})

(defattr label-name :label/name :string
  {::attr/schema :tracker
   ::attr/identities #{:label/id}
   ::attr/required? true
   ::pg2/max-length 50})

(defattr label-description :label/description :string
  {::attr/schema :tracker
   ::attr/identities #{:label/id}
   ::pg2/max-length 200})

;; Color as keyword (e.g., :color/red, :color/blue)
(defattr label-color :label/color :keyword
  {::attr/schema :tracker
   ::attr/identities #{:label/id}})

;; Position for ordering
(defattr label-position :label/position :int
  {::attr/schema :tracker
   ::attr/identities #{:label/id}})

;; To-one: Label belongs to project
(defattr label-project :label/project :ref
  {::attr/cardinality :one
   ::attr/target :project/id
   ::attr/schema :tracker
   ::attr/identities #{:label/id}})

(def label-attributes
  [label-id label-name label-description label-color label-position label-project])

;; =============================================================================
;; Milestone - Project milestones
;; =============================================================================

(defattr milestone-id :milestone/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "milestones"})

(defattr milestone-name :milestone/name :string
  {::attr/schema :tracker
   ::attr/identities #{:milestone/id}
   ::attr/required? true
   ::pg2/max-length 100})

(defattr milestone-description :milestone/description :string
  {::attr/schema :tracker
   ::attr/identities #{:milestone/id}
   ::pg2/max-length 2000})

(def milestone-statuses #{:milestone.status/open :milestone.status/closed})

(defattr milestone-status :milestone/status :enum
  {::attr/schema :tracker
   ::attr/identities #{:milestone/id}
   ::attr/enumerated-values milestone-statuses})

(defattr milestone-due-date :milestone/due-date :instant
  {::attr/schema :tracker
   ::attr/identities #{:milestone/id}
   ::pg2/column-name "due_date"})

(defattr milestone-closed-at :milestone/closed-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:milestone/id}
   ::pg2/column-name "closed_at"})

;; To-one: Milestone belongs to project
(defattr milestone-project :milestone/project :ref
  {::attr/cardinality :one
   ::attr/target :project/id
   ::attr/schema :tracker
   ::attr/identities #{:milestone/id}})

;; To-many: Milestone has many issues
(defattr milestone-issues :milestone/issues :ref
  {::attr/cardinality :many
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:milestone/id}
   ::pg2/fk-attr :issue/milestone})

(def milestone-attributes
  [milestone-id milestone-name milestone-description milestone-status
   milestone-due-date milestone-closed-at milestone-project milestone-issues])

;; =============================================================================
;; Issue - The core entity (uses :long ID for sequential issue numbers)
;; =============================================================================

(defattr issue-id :issue/id :long
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "issues"})

(defattr issue-title :issue/title :string
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::attr/required? true
   ::pg2/max-length 200})

(defattr issue-description :issue/description :string
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/max-length 10000})

(def issue-statuses #{:issue.status/open :issue.status/in-progress
                      :issue.status/review :issue.status/closed})

(defattr issue-status :issue/status :enum
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::attr/enumerated-values issue-statuses})

(def issue-priorities #{:issue.priority/critical :issue.priority/high
                        :issue.priority/medium :issue.priority/low})

(defattr issue-priority :issue/priority :enum
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::attr/enumerated-values issue-priorities})

(def issue-types #{:issue.type/bug :issue.type/feature :issue.type/task
                   :issue.type/improvement :issue.type/epic})

(defattr issue-type :issue/type :enum
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::attr/enumerated-values issue-types
   ::pg2/column-name "issue_type"})

;; Workflow state as symbol (for state machine integration)
(defattr issue-workflow-state :issue/workflow-state :symbol
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "workflow_state"})

;; Numeric priority for ordering
(defattr issue-priority-order :issue/priority-order :int
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "priority_order"})

;; Time tracking
(defattr issue-estimate :issue/estimate :decimal
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}})

(defattr issue-time-spent :issue/time-spent :decimal
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "time_spent"})

;; Vote count (denormalized for performance)
(defattr issue-vote-count :issue/vote-count :int
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "vote_count"})

;; Timestamps
(defattr issue-created-at :issue/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "created_at"})

(defattr issue-updated-at :issue/updated-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "updated_at"})

(defattr issue-closed-at :issue/closed-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "closed_at"})

(defattr issue-due-date :issue/due-date :instant
  {::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "due_date"})

;; To-one: Issue belongs to project
(defattr issue-project :issue/project :ref
  {::attr/cardinality :one
   ::attr/target :project/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}})

;; To-one: Issue optionally belongs to milestone
(defattr issue-milestone :issue/milestone :ref
  {::attr/cardinality :one
   ::attr/target :milestone/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}})

;; To-one: Issue reporter (user)
(defattr issue-reporter :issue/reporter :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "reporter_id"})

;; Self-referential: Parent issue (for sub-issues)
(defattr issue-parent :issue/parent :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/column-name "parent_id"})

;; Self-referential: Child issues
(defattr issue-children :issue/children :ref
  {::attr/cardinality :many
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :issue/parent
   ::pg2/order-by :issue/priority-order})

;; To-many: Issue has many comments
(defattr issue-comments :issue/comments :ref
  {::attr/cardinality :many
   ::attr/target :comment/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :comment/issue
   ::pg2/order-by :comment/created-at})

;; To-many: Issue has many attachments (owned)
(defattr issue-attachments :issue/attachments :ref
  {::attr/cardinality :many
   ::attr/target :attachment/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :attachment/issue
   ::pg2/delete-orphan? true})

;; To-many: Issue has many time entries
(defattr issue-time-entries :issue/time-entries :ref
  {::attr/cardinality :many
   ::attr/target :time-entry/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :time-entry/issue
   ::pg2/order-by :time-entry/logged-at})

;; To-many: Issue labels (via join table - M:M #1)
(defattr issue-labels :issue/labels :ref
  {::attr/cardinality :many
   ::attr/target :issue-label/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :issue-label/issue})

;; To-many: Issue watchers (via join table - M:M #2)
(defattr issue-watchers :issue/watchers :ref
  {::attr/cardinality :many
   ::attr/target :issue-watcher/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :issue-watcher/issue})

;; To-many: Issue assignees (via join table - M:M #3)
(defattr issue-assignees :issue/assignees :ref
  {::attr/cardinality :many
   ::attr/target :issue-assignee/id
   ::attr/schema :tracker
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :issue-assignee/issue})

(def issue-attributes
  [issue-id issue-title issue-description issue-status issue-priority
   issue-type issue-workflow-state issue-priority-order issue-estimate
   issue-time-spent issue-vote-count issue-created-at issue-updated-at
   issue-closed-at issue-due-date issue-project issue-milestone issue-reporter
   issue-parent issue-children issue-comments issue-attachments issue-time-entries
   issue-labels issue-watchers issue-assignees])

;; =============================================================================
;; IssueLabel - Join table: Issue ↔ Label (many-to-many #1)
;; =============================================================================

(defattr issue-label-id :issue-label/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "issue_labels"})

(defattr issue-label-added-at :issue-label/added-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:issue-label/id}
   ::pg2/column-name "added_at"})

;; To-one: IssueLabel → Issue
(defattr issue-label-issue :issue-label/issue :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:issue-label/id}})

;; To-one: IssueLabel → Label
(defattr issue-label-label :issue-label/label :ref
  {::attr/cardinality :one
   ::attr/target :label/id
   ::attr/schema :tracker
   ::attr/identities #{:issue-label/id}})

(def issue-label-attributes
  [issue-label-id issue-label-added-at issue-label-issue issue-label-label])

;; =============================================================================
;; IssueWatcher - Join table: Issue ↔ User watchers (many-to-many #2)
;; =============================================================================

(defattr issue-watcher-id :issue-watcher/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "issue_watchers"})

(defattr issue-watcher-subscribed-at :issue-watcher/subscribed-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:issue-watcher/id}
   ::pg2/column-name "subscribed_at"})

;; Notification preferences (as keywords CSV)
(defattr issue-watcher-notify-on :issue-watcher/notify-on :string
  {::attr/schema :tracker
   ::attr/identities #{:issue-watcher/id}
   ::pg2/max-length 200
   ::pg2/column-name "notify_on"
   ::pg2/form->sql-value tags->csv
   ::pg2/sql->form-value csv->tags})

;; To-one: IssueWatcher → Issue
(defattr issue-watcher-issue :issue-watcher/issue :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:issue-watcher/id}})

;; To-one: IssueWatcher → User
(defattr issue-watcher-user :issue-watcher/user :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:issue-watcher/id}})

(def issue-watcher-attributes
  [issue-watcher-id issue-watcher-subscribed-at issue-watcher-notify-on
   issue-watcher-issue issue-watcher-user])

;; =============================================================================
;; IssueAssignee - Join table: Issue ↔ User assignees (many-to-many #3)
;; =============================================================================

(defattr issue-assignee-id :issue-assignee/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "issue_assignees"})

(defattr issue-assignee-assigned-at :issue-assignee/assigned-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:issue-assignee/id}
   ::pg2/column-name "assigned_at"})

;; Whether this is the primary assignee
(defattr issue-assignee-primary? :issue-assignee/primary? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:issue-assignee/id}
   ::pg2/column-name "is_primary"})

;; To-one: IssueAssignee → Issue
(defattr issue-assignee-issue :issue-assignee/issue :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:issue-assignee/id}})

;; To-one: IssueAssignee → User
(defattr issue-assignee-user :issue-assignee/user :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:issue-assignee/id}})

(def issue-assignee-attributes
  [issue-assignee-id issue-assignee-assigned-at issue-assignee-primary?
   issue-assignee-issue issue-assignee-user])

;; =============================================================================
;; Comment - Issue comments (Level 4 in query depth)
;; =============================================================================

(defattr comment-id :comment/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "comments"})

(defattr comment-body :comment/body :string
  {::attr/schema :tracker
   ::attr/identities #{:comment/id}
   ::attr/required? true
   ::pg2/max-length 10000})

(defattr comment-created-at :comment/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:comment/id}
   ::pg2/column-name "created_at"})

(defattr comment-updated-at :comment/updated-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:comment/id}
   ::pg2/column-name "updated_at"})

(defattr comment-edited? :comment/edited? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:comment/id}
   ::pg2/column-name "is_edited"})

;; To-one: Comment belongs to issue
(defattr comment-issue :comment/issue :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:comment/id}})

;; To-one: Comment author
(defattr comment-author :comment/author :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:comment/id}})

;; To-many: Comment has many reactions (Level 5!)
(defattr comment-reactions :comment/reactions :ref
  {::attr/cardinality :many
   ::attr/target :reaction/id
   ::attr/schema :tracker
   ::attr/identities #{:comment/id}
   ::pg2/fk-attr :reaction/comment
   ::pg2/delete-orphan? true})

;; To-many: Comment can have attachments
(defattr comment-attachments :comment/attachments :ref
  {::attr/cardinality :many
   ::attr/target :attachment/id
   ::attr/schema :tracker
   ::attr/identities #{:comment/id}
   ::pg2/fk-attr :attachment/comment
   ::pg2/delete-orphan? true})

(def comment-attributes
  [comment-id comment-body comment-created-at comment-updated-at comment-edited?
   comment-issue comment-author comment-reactions comment-attachments])

;; =============================================================================
;; Reaction - Comment reactions (Level 5 in query depth)
;; =============================================================================

(defattr reaction-id :reaction/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "reactions"})

(def reaction-types #{:reaction.type/thumbs-up :reaction.type/thumbs-down
                      :reaction.type/heart :reaction.type/laugh
                      :reaction.type/confused :reaction.type/celebrate})

(defattr reaction-type :reaction/type :enum
  {::attr/schema :tracker
   ::attr/identities #{:reaction/id}
   ::attr/enumerated-values reaction-types
   ::pg2/column-name "reaction_type"})

(defattr reaction-created-at :reaction/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:reaction/id}
   ::pg2/column-name "created_at"})

;; To-one: Reaction belongs to comment
(defattr reaction-comment :reaction/comment :ref
  {::attr/cardinality :one
   ::attr/target :comment/id
   ::attr/schema :tracker
   ::attr/identities #{:reaction/id}})

;; To-one: Reaction author
(defattr reaction-user :reaction/user :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:reaction/id}})

(def reaction-attributes
  [reaction-id reaction-type reaction-created-at reaction-comment reaction-user])

;; =============================================================================
;; Attachment - File attachments (owned by issue or comment)
;; =============================================================================

(defattr attachment-id :attachment/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "attachments"})

(defattr attachment-filename :attachment/filename :string
  {::attr/schema :tracker
   ::attr/identities #{:attachment/id}
   ::attr/required? true
   ::pg2/max-length 255})

(defattr attachment-content-type :attachment/content-type :string
  {::attr/schema :tracker
   ::attr/identities #{:attachment/id}
   ::pg2/max-length 100
   ::pg2/column-name "content_type"})

(defattr attachment-size :attachment/size :long
  {::attr/schema :tracker
   ::attr/identities #{:attachment/id}})

(defattr attachment-storage-key :attachment/storage-key :string
  {::attr/schema :tracker
   ::attr/identities #{:attachment/id}
   ::pg2/max-length 500
   ::pg2/column-name "storage_key"})

(defattr attachment-created-at :attachment/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:attachment/id}
   ::pg2/column-name "created_at"})

;; To-one: Optional attachment to issue
(defattr attachment-issue :attachment/issue :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:attachment/id}})

;; To-one: Optional attachment to comment
(defattr attachment-comment :attachment/comment :ref
  {::attr/cardinality :one
   ::attr/target :comment/id
   ::attr/schema :tracker
   ::attr/identities #{:attachment/id}})

;; To-one: Uploaded by user
(defattr attachment-uploader :attachment/uploader :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:attachment/id}})

(def attachment-attributes
  [attachment-id attachment-filename attachment-content-type attachment-size
   attachment-storage-key attachment-created-at attachment-issue
   attachment-comment attachment-uploader])

;; =============================================================================
;; TimeEntry - Time tracking entries
;; =============================================================================

(defattr time-entry-id :time-entry/id :uuid
  {::attr/identity? true
   ::attr/schema :tracker
   ::pg2/table "time_entries"})

(defattr time-entry-description :time-entry/description :string
  {::attr/schema :tracker
   ::attr/identities #{:time-entry/id}
   ::pg2/max-length 500})

(defattr time-entry-hours :time-entry/hours :decimal
  {::attr/schema :tracker
   ::attr/identities #{:time-entry/id}})

(defattr time-entry-logged-at :time-entry/logged-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:time-entry/id}
   ::pg2/column-name "logged_at"})

(defattr time-entry-created-at :time-entry/created-at :instant
  {::attr/schema :tracker
   ::attr/identities #{:time-entry/id}
   ::pg2/column-name "created_at"})

;; Billable flag
(defattr time-entry-billable? :time-entry/billable? :boolean
  {::attr/schema :tracker
   ::attr/identities #{:time-entry/id}
   ::pg2/column-name "is_billable"})

;; To-one: TimeEntry belongs to issue
(defattr time-entry-issue :time-entry/issue :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :tracker
   ::attr/identities #{:time-entry/id}})

;; To-one: TimeEntry logged by user
(defattr time-entry-user :time-entry/user :ref
  {::attr/cardinality :one
   ::attr/target :user/id
   ::attr/schema :tracker
   ::attr/identities #{:time-entry/id}})

(def time-entry-attributes
  [time-entry-id time-entry-description time-entry-hours time-entry-logged-at
   time-entry-created-at time-entry-billable? time-entry-issue time-entry-user])

;; =============================================================================
;; All Attributes
;; =============================================================================

(def all-attributes
  (concat organization-attributes
          team-attributes
          user-attributes
          api-token-attributes
          notification-attributes
          team-member-attributes
          project-attributes
          project-member-attributes
          webhook-attributes
          label-attributes
          milestone-attributes
          issue-attributes
          issue-label-attributes
          issue-watcher-attributes
          issue-assignee-attributes
          comment-attributes
          reaction-attributes
          attachment-attributes
          time-entry-attributes))

;; =============================================================================
;; Entity Summary
;; =============================================================================
;;
;; Total: 19 entities, 120+ attributes
;;
;; Query depth examples (5 levels):
;;   1. Organization → Project → Issue → Comment → Reaction
;;   2. Organization → Team → TeamMember → User → Notification
;;   3. Project → Milestone → Issue → TimeEntry → User
;;   4. User → ProjectMember → Project → Issue → Attachment
;;
;; Many-to-many relationships (3):
;;   1. Issue ↔ Label (via IssueLabel)
;;   2. Issue ↔ User watchers (via IssueWatcher)
;;   3. Issue ↔ User assignees (via IssueAssignee)
;;
;; Additional M:M patterns:
;;   - User ↔ Team (via TeamMember with role)
;;   - User ↔ Project (via ProjectMember with role)
;;
;; delete-orphan? usage:
;;   - User → ApiToken
;;   - Project → Webhook
;;   - Issue → Attachment
;;   - Comment → Reaction
;;   - Comment → Attachment
;;
;; Custom transformers:
;;   - ApiToken permissions (JSON encode/decode)
;;   - Webhook events (CSV tags)
;;   - IssueWatcher notify-on (CSV tags)
;;
;; Self-referential:
;;   - Issue parent/children (hierarchy)
;;
