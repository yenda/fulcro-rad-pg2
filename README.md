# fulcro-rad-pg2

A high-performance PostgreSQL adapter for [Fulcro RAD](https://github.com/fulcrologic/fulcro-rad), using the native [pg2](https://github.com/igrishaev/pg2) driver.

## Features

- **Native PostgreSQL driver** - Uses pg2 instead of JDBC for 70-99% faster reads
- **Automatic schema generation** - Generate tables from RAD attributes
- **Pathom3 resolver generation** - Auto-generate resolvers for all attributes
- **Bidirectional transformers** - Custom encode/decode for complex types (JSON, CSV, etc.)
- **Ordered collections** - Sort to-many relationships by any attribute
- **Cascade delete** - Automatic orphan cleanup for owned relationships

## Installation

Add to your `deps.edn`:

```clojure
{:deps {yenda/fulcro-rad-pg2 {:mvn/version "0.1.0"}}}
```

## Quick Start

### 1. Define Attributes

```clojure
(ns myapp.model
  (:require
   [com.fulcrologic.rad.attributes :refer [defattr]]
   [com.fulcrologic.rad.database-adapters.pg2 :as pg2]))

(defattr user-id :user/id :uuid
  {::attr/identity? true
   ::attr/schema :production
   ::pg2/table "users"})

(defattr user-name :user/name :string
  {::attr/schema :production
   ::attr/identities #{:user/id}
   ::pg2/max-length 100})

(defattr user-email :user/email :string
  {::attr/schema :production
   ::attr/identities #{:user/id}
   ::pg2/column-name "email_address"})
```

### 2. Configure Database

```clojure
;; config.edn
{::pg2/databases
 {:main {:pg2/config {:host "localhost"
                      :port 5432
                      :user "myuser"
                      :password "mypass"
                      :database "mydb"}
         :pg2/pool {:pool-min-size 2
                    :pool-max-size 10}
         :pg2/schema :production}}}
```

### 3. Generate Schema

```clojure
(require '[com.fulcrologic.rad.database-adapters.pg2.migration :as mig])

;; Auto-generate SQL for all attributes
(mig/automatic-schema :production all-attributes)
```

### 4. Set Up Plugin

```clojure
(require '[com.fulcrologic.rad.database-adapters.pg2.plugin :as pg2-plugin])

;; In your Pathom parser setup
(pg2-plugin/wrap-pg2-save)
```

## Attribute Options Reference

### `::pg2/table`

Specifies the database table name for an entity. Required on identity attributes.

```clojure
(defattr user-id :user/id :uuid
  {::attr/identity? true
   ::attr/schema :production
   ::pg2/table "users"})         ; → CREATE TABLE users ...
```

**Default:** Derived from attribute namespace (e.g., `:user/id` → `"user"`)

---

### `::pg2/column-name`

Override the database column name. Useful for snake_case conventions or reserved words.

```clojure
(defattr user-active? :user/active? :boolean
  {::attr/schema :production
   ::attr/identities #{:user/id}
   ::pg2/column-name "is_active"})  ; → is_active BOOLEAN

(defattr user-created-at :user/created-at :instant
  {::attr/schema :production
   ::attr/identities #{:user/id}
   ::pg2/column-name "created_at"}) ; → created_at TIMESTAMP WITH TIME ZONE
```

**Default:** Derived from attribute name using snake_case (e.g., `:user/display-name` → `"display_name"`)

---

### `::pg2/max-length`

Set the maximum length for VARCHAR columns. Applies to `:string`, `:password`, `:keyword`, `:symbol`, and `:enum` types.

```clojure
(defattr user-email :user/email :string
  {::attr/schema :production
   ::attr/identities #{:user/id}
   ::pg2/max-length 255})           ; → VARCHAR(255)

(defattr issue-description :issue/description :string
  {::attr/schema :production
   ::attr/identities #{:issue/id}
   ::pg2/max-length 10000})         ; → VARCHAR(10000)
```

**Defaults:**
- `:string` → `VARCHAR(200)`
- `:password` → `VARCHAR(512)`
- `:enum`, `:keyword`, `:symbol` → `VARCHAR(200)`

---

### `::pg2/fk-attr`

Specifies which attribute owns the foreign key in a to-many relationship. This tells the resolver where to find the FK column.

```clojure
;; The "many" side - Organization has many projects
(defattr org-projects :organization/projects :ref
  {::attr/cardinality :many
   ::attr/target :project/id
   ::attr/schema :production
   ::attr/identities #{:organization/id}
   ::pg2/fk-attr :project/organization})  ; FK is on projects.organization

;; The "one" side - Project belongs to organization
(defattr project-org :project/organization :ref
  {::attr/cardinality :one
   ::attr/target :organization/id
   ::attr/schema :production
   ::attr/identities #{:project/id}})     ; FK column created here
```

**Usage:** Required on `:many` cardinality refs. Points to the attribute on the target entity that has the FK column.

---

### `::pg2/delete-orphan?`

Automatically delete child entities when removed from a to-many relationship. Use for "owned" or "component" relationships.

```clojure
;; When API tokens are removed from user, delete them
(defattr user-tokens :user/api-tokens :ref
  {::attr/cardinality :many
   ::attr/target :api-token/id
   ::attr/schema :production
   ::attr/identities #{:user/id}
   ::pg2/fk-attr :api-token/user
   ::pg2/delete-orphan? true})            ; Tokens deleted when removed

;; When attachments are removed from issue, delete them
(defattr issue-attachments :issue/attachments :ref
  {::attr/cardinality :many
   ::attr/target :attachment/id
   ::attr/schema :production
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :attachment/issue
   ::pg2/delete-orphan? true})            ; Attachments deleted when removed
```

**Default:** `false` - child entities are orphaned but not deleted

**When to use:**
- File attachments owned by a document
- API tokens owned by a user
- Reactions owned by a comment
- Any "composition" relationship where children don't exist independently

---

### `::pg2/order-by`

Sort to-many collections by a specific attribute. The attribute must exist on the target entity.

```clojure
;; Comments sorted by creation time
(defattr issue-comments :issue/comments :ref
  {::attr/cardinality :many
   ::attr/target :comment/id
   ::attr/schema :production
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :comment/issue
   ::pg2/order-by :comment/created-at})   ; ORDER BY created_at ASC

;; Labels sorted by position
(defattr project-labels :project/labels :ref
  {::attr/cardinality :many
   ::attr/target :label/id
   ::attr/schema :production
   ::attr/identities #{:project/id}
   ::pg2/fk-attr :label/project
   ::pg2/order-by :label/position})       ; ORDER BY position ASC

;; Milestones sorted by due date
(defattr project-milestones :project/milestones :ref
  {::attr/cardinality :many
   ::attr/target :milestone/id
   ::attr/schema :production
   ::attr/identities #{:project/id}
   ::pg2/fk-attr :milestone/project
   ::pg2/order-by :milestone/due-date})   ; ORDER BY due_date ASC
```

**Default:** No ordering (database default order)

**Note:** Currently only supports ascending order. NULL values sort last (PostgreSQL default).

---

### `::pg2/form->sql-value`

Custom transformer function to convert Clojure values to SQL on write. Use for complex types that need serialization.

```clojure
;; JSON encoding for map/vector data
(defn json-encode [data]
  (when data
    (jsonista/write-value-as-string data)))

(defattr token-permissions :api-token/permissions :string
  {::attr/schema :production
   ::attr/identities #{:api-token/id}
   ::pg2/max-length 1000
   ::pg2/form->sql-value json-encode})    ; {:read true} → "{\"read\":true}"

;; CSV encoding for tag vectors
(defn tags->csv [tags]
  (when (seq tags)
    (str/join "," (map name tags))))

(defattr webhook-events :webhook/events :string
  {::attr/schema :production
   ::attr/identities #{:webhook/id}
   ::pg2/max-length 500
   ::pg2/form->sql-value tags->csv})      ; [:push :pr] → "push,pr"
```

**Common use cases:**
- JSON serialization for maps/vectors
- CSV encoding for keyword lists
- Encryption for sensitive data
- Custom date formatting

---

### `::pg2/sql->form-value`

Custom transformer function to convert SQL values to Clojure on read. Paired with `::pg2/form->sql-value` for bidirectional transformation.

```clojure
;; JSON decoding
(defn json-decode [json-str]
  (when json-str
    (jsonista/read-value json-str)))

(defattr token-permissions :api-token/permissions :string
  {::attr/schema :production
   ::attr/identities #{:api-token/id}
   ::pg2/max-length 1000
   ::pg2/form->sql-value json-encode
   ::pg2/sql->form-value json-decode})    ; "{\"read\":true}" → {:read true}

;; CSV decoding
(defn csv->tags [csv]
  (when (and csv (not (str/blank? csv)))
    (mapv keyword (str/split csv #","))))

(defattr webhook-events :webhook/events :string
  {::attr/schema :production
   ::attr/identities #{:webhook/id}
   ::pg2/max-length 500
   ::pg2/form->sql-value tags->csv
   ::pg2/sql->form-value csv->tags})      ; "push,pr" → [:push :pr]
```

**Note:** Custom transformers take precedence over built-in type decoders (for `:instant`, `:enum`, `:keyword`, `:symbol`).

## Supported Types

| RAD Type    | PostgreSQL Type              | Notes                          |
|-------------|------------------------------|--------------------------------|
| `:uuid`     | `UUID`                       | Default for identity columns   |
| `:long`     | `BIGINT`                     | With auto-increment sequence   |
| `:int`      | `INTEGER`                    |                                |
| `:string`   | `VARCHAR(n)`                 | Default 200, use `max-length`  |
| `:password` | `VARCHAR(512)`               | For hashed passwords           |
| `:boolean`  | `BOOLEAN`                    |                                |
| `:decimal`  | `DECIMAL(20,2)`              |                                |
| `:instant`  | `TIMESTAMP WITH TIME ZONE`   | Auto-converts to/from Instant  |
| `:enum`     | `VARCHAR(200)`               | Stored as keyword string       |
| `:keyword`  | `VARCHAR(200)`               | Stored with leading `:`        |
| `:symbol`   | `VARCHAR(200)`               |                                |
| `:ref`      | FK column (target type)      | Creates FK constraint          |

## Relationship Patterns

### To-One (Forward)

The current entity owns the FK column:

```clojure
(defattr project-organization :project/organization :ref
  {::attr/cardinality :one
   ::attr/target :organization/id
   ::attr/schema :production
   ::attr/identities #{:project/id}})
;; → projects.organization UUID REFERENCES organizations(id)
```

### To-One (Reverse)

Query from parent to single child where child owns FK:

```clojure
;; On User entity - user has one profile
(defattr user-profile :user/profile :ref
  {::attr/cardinality :one
   ::attr/target :profile/id
   ::attr/schema :production
   ::attr/identities #{:user/id}
   ::pg2/fk-attr :profile/user})  ; FK is on profiles.user_id
```

### To-Many

Query from parent to multiple children where children own FK:

```clojure
(defattr project-issues :project/issues :ref
  {::attr/cardinality :many
   ::attr/target :issue/id
   ::attr/schema :production
   ::attr/identities #{:project/id}
   ::pg2/fk-attr :issue/project    ; FK is on issues.project
   ::pg2/order-by :issue/created-at})
```

### Many-to-Many (via Join Table)

Create an explicit join entity:

```clojure
;; Join entity
(defattr issue-label-id :issue-label/id :uuid
  {::attr/identity? true
   ::attr/schema :production
   ::pg2/table "issue_labels"})

(defattr issue-label-issue :issue-label/issue :ref
  {::attr/cardinality :one
   ::attr/target :issue/id
   ::attr/schema :production
   ::attr/identities #{:issue-label/id}})

(defattr issue-label-label :issue-label/label :ref
  {::attr/cardinality :one
   ::attr/target :label/id
   ::attr/schema :production
   ::attr/identities #{:issue-label/id}})

;; Access from Issue
(defattr issue-labels :issue/labels :ref
  {::attr/cardinality :many
   ::attr/target :issue-label/id
   ::attr/schema :production
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :issue-label/issue})
```

### Self-Referential

For hierarchies (parent/children):

```clojure
;; To-one: Child → Parent
(defattr issue-parent :issue/parent :ref
  {::attr/cardinality :one
   ::attr/target :issue/id           ; Self-reference
   ::attr/schema :production
   ::attr/identities #{:issue/id}
   ::pg2/column-name "parent_id"})

;; To-many: Parent → Children
(defattr issue-children :issue/children :ref
  {::attr/cardinality :many
   ::attr/target :issue/id           ; Self-reference
   ::attr/schema :production
   ::attr/identities #{:issue/id}
   ::pg2/fk-attr :issue/parent       ; FK is parent_id column
   ::pg2/order-by :issue/priority-order})
```

## Migration from fulcro-rad-sql

See [MIGRATION.md](MIGRATION.md) for a complete guide on migrating from `fulcro-rad-sql`.

Key changes:
- Namespace: `database-adapters.sql` → `database-adapters.pg2`
- Config keys: `::sql/*` → `::pg2/*`
- Driver: next.jdbc → pg2 (parameter syntax `$1` vs `?`)
- Middleware: `wrap-sql-save` → `wrap-pg2-save`

## License

Copyright 2024 Fulcrologic, LLC

Distributed under the MIT License.
