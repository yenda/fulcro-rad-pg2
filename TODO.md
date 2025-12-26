# fulcro-rad-pg2 TODO

Gaps identified during integration testing of the example issue tracker model.

## Adapter Bugs

### 1. ~~`sql->form-value` not applied on reads~~ FIXED

**Status:** FIXED
**Location:** `src/main/com/fulcrologic/rad/database_adapters/pg2/read.clj`, `driver.clj`
**Test:** `sql->form-value-applied-on-reads-test`

**Fix applied:**
- `read.clj`: Updated `build-pg2-column-config` to include `sql->form-value` transformer from attributes
- `driver.clj`: Updated `compile-pg2-row-transformer` to apply custom transformer (takes precedence over type-based decoder)

Custom bidirectional transformers (JSON, CSV, encryption, etc.) now work correctly on both reads and writes.

---

### 2. ~~Empty to-many results throw Pathom errors~~ FIXED

**Status:** FIXED
**Location:** `src/main/com/fulcrologic/rad/database_adapters/pg2/read.clj`
**Test:** `empty-to-many-returns-empty-vector-test`

**Fix applied:**
- `build-to-many-resolver-config`: Added `:empty-result {attr-k []}` to config
- `to-many-resolver`: Use `(get results-by-id id empty-result)` instead of `(results-by-id id)` to return empty vector for IDs with no results

To-many relationships with no results now correctly return `{:attr-key []}` instead of throwing.

---

## Missing Test Coverage

### 3. ~~Update operations~~ DONE

**Status:** DONE
**Tests added:**
- `update-scalar-values-test` - Verifies updating multiple scalar fields (title, status, priority)
- `update-to-one-ref-test` - Verifies re-parenting (moving issue to different project)
- `update-partial-fields-test` - Verifies partial updates only change specified fields

---

### 4. ~~Null/nil handling~~ DONE

**Status:** DONE
**Bug fixed:** `compile-pg2-row-transformer` now uses `contains?` instead of `if-some` to include NULL values in output maps.
**Tests added:**
- `set-optional-string-to-nil-test` - Set display-name to nil
- `set-optional-instant-to-nil-test` - Set last-login-at to nil
- `set-optional-decimal-to-nil-test` - Set budget to nil
- `clear-optional-to-one-ref-test` - Clear issue/milestone ref

---

### 5. ~~Batch resolver behavior~~ DONE

**Status:** DONE
**Tests added:**
- `batch-resolution-multiple-entities-test` - Query 10 entities in single batch, verify correct data
- `batch-resolution-with-joins-test` - Batch query with to-one joins
- `batch-to-many-resolution-test` - To-many with varying child counts (2, 3, 0)

---

### 6. ~~Error scenarios~~ DONE

**Status:** DONE
**Tests added:**
- `fk-violation-throws-exception-test` - FK violation throws exception
- `duplicate-allowed-without-unique-constraint-test` - Documents that schema generator doesn't create UNIQUE constraints (Fulcro RAD `::attr/required?` is form-level only)
- `null-allowed-without-not-null-constraint-test` - Documents that schema generator doesn't create NOT NULL constraints

---

### 7. ~~`order-by` edge cases~~ DONE

**Status:** DONE
**Tests added:**
- `order-by-with-null-values-test` - NULL values sorted last (PostgreSQL default)
- `order-by-duplicate-values-test` - Duplicate positions maintain grouping
- `order-by-with-large-dataset-test` - 20 items sorted correctly
- `order-by-empty-collection-test` - Empty result returns `[]`
- `order-by-single-item-test` - Single item works
- `order-by-negative-positions-test` - Negative/zero/positive sorting
- `order-by-batch-multiple-parents-test` - Batch query with independent ordering per parent

---

### 8. ~~Transformer edge cases~~ DONE

**Status:** DONE
**Tests added:**

CSV transformer (tags->csv / csv->tags):
- `transformer-csv-nil-value-test` - nil value handling
- `transformer-csv-empty-vector-test` - empty vector -> nil
- `transformer-csv-single-tag-test` - single tag round-trip
- `transformer-csv-multiple-tags-test` - multiple tags preserve order

JSON transformer (json-encode / json-decode):
- `transformer-json-nil-value-test` - nil value handling
- `transformer-json-empty-map-test` - empty map {} round-trip
- `transformer-json-simple-map-test` - simple map round-trip
- `transformer-json-nested-structure-test` - nested structures
- `transformer-json-vector-value-test` - vector values

Update operations:
- `transformer-update-value-test` - updating transformed values

---

## Additional Test Coverage

### 9. ~~Delete operations~~ DONE

**Status:** DONE
**Tests added:**
- `delete-single-entity-test` - Basic entity deletion
- `delete-entity-with-children-fk-violation-test` - FK constraint enforcement
- `delete-multiple-entities-test` - Batch deletion in single delta
- `delete-nonexistent-entity-test` - Idempotent delete behavior
- `delete-entity-then-query-test` - Verification after deletion
- `delete-parent-with-to-many-children-test` - FK constraint on parent
- `delete-leaf-entity-preserves-parent-test` - Child deletion preserves parent
- `delete-and-create-in-same-delta-test` - Mixed create/delete operations

Note: `delete-orphan?` already tested extensively in `delete_orphan_edge_cases_test.clj`

### 10. ~~Self-referential relationships~~ DONE

**Status:** DONE
**Tests added:**
- `self-ref-query-parent-from-child-test` - Query parent issue from child via `:issue/parent`
- `self-ref-query-children-from-parent-test` - Query children with `::pg2/order-by` ordering
- `self-ref-multiple-levels-test` - 3-level hierarchy (Epic → Story → Task), top-down and bottom-up
- `self-ref-no-parent-test` - Standalone issue has NULL parent_id
- `self-ref-no-children-test` - Leaf issue returns empty `[]` for children
- `self-ref-reparent-issue-test` - Move child to different parent
- `self-ref-batch-query-test` - Batch query multiple parents with independent children

### 11. ~~Many-to-many through join table~~ DONE

**Status:** DONE
**Tests added:**
- `m2m-query-labels-for-issue-test` - Query labels via `:issue/labels` → IssueLabel → Label
- `m2m-query-issues-for-label-test` - Reverse lookup via SQL (no direct resolver)
- `m2m-add-label-to-issue-test` - Add label by creating IssueLabel entity
- `m2m-remove-label-from-issue-test` - Remove label by deleting IssueLabel entity
- `m2m-issue-with-no-labels-test` - Empty labels returns `[]`
- `m2m-multiple-issues-same-label-test` - Shared label across 3 issues
- `m2m-one-issue-multiple-labels-test` - Issue with 5 labels
- `m2m-batch-query-issues-with-labels-test` - Batch query with joins

---

## Automigration Testing

### 12. ~~Schema introspection tests~~ DONE

**Status:** DONE
**Tests added:**
- `schema-column-types-uuid-test` - UUID identity columns have `uuid` type
- `schema-column-types-bigint-test` - Long identity columns have `int8` (BIGINT) type
- `schema-column-types-varchar-test` - VARCHAR with correct max-length (200 default, 50 for label/name)
- `schema-column-types-boolean-test` - Boolean columns have `bool` type
- `schema-column-types-timestamp-test` - Instant columns have `timestamptz` type
- `schema-column-types-decimal-test` - Decimal columns have `numeric` type
- `schema-column-types-int-test` - Int columns have `int4` (INTEGER) type
- `schema-sequence-exists-for-long-identity-test` - Sequences created for long identity columns
- `schema-no-sequence-for-uuid-identity-test` - No sequences for UUID identity columns
- `schema-index-on-identity-column-test` - Indexes on identity columns
- `schema-fk-constraints-exist-test` - FK constraints exist on reference columns
- `schema-fk-index-on-reference-column-test` - Indexes on FK reference columns
- `schema-multiple-fk-constraints-test` - Multiple FK constraints on tables with multiple refs
- `schema-all-expected-tables-exist-test` - All expected tables from model are created

### 13. ~~Constraint verification tests~~ DONE

**Status:** DONE
**Tests added:**

Constraints that ARE created:
- `constraint-unique-index-on-uuid-identity-test` - Unique index on UUID identity columns
- `constraint-unique-index-on-long-identity-test` - Unique index on long identity columns
- `constraint-primary-key-not-created-test` - PRIMARY KEY constraint NOT created (only index)
- `constraint-fk-deferrable-test` - FK constraints are DEFERRABLE INITIALLY DEFERRED

Constraints that are NOT created:
- `constraint-not-null-not-created-test` - NOT NULL not created (::attr/required? is form-level only)
- `constraint-unique-not-created-on-non-id-test` - UNIQUE not created on non-id columns
- `constraint-check-not-created-test` - CHECK constraints not created

VARCHAR max-length verification:
- `constraint-varchar-default-length-test` - Keywords/enums default to VARCHAR(200)
- `constraint-varchar-custom-length-test` - ::pg2/max-length produces correct VARCHAR(n)
- `constraint-varchar-large-max-length-test` - Large max-length (10000) works correctly

Index verification:
- `constraint-index-on-fk-columns-test` - Indexes created on FK reference columns

### 14. ~~Migration diff testing~~ DONE

**Status:** DONE
**Tests added:**

Idempotency tests:
- `migration-idempotency-test` - Re-running automatic-schema is safe (IF NOT EXISTS)
- `migration-idempotency-tables-unchanged-test` - Table count unchanged after second run
- `migration-idempotency-columns-unchanged-test` - Column count unchanged after second run
- `migration-idempotency-indexes-unchanged-test` - Index count unchanged after second run

Schema modification tests:
- `migration-add-column-to-existing-table-test` - ADD COLUMN IF NOT EXISTS works
- `migration-add-column-if-not-exists-test` - IF NOT EXISTS on columns
- `migration-create-table-if-not-exists-test` - IF NOT EXISTS on tables
- `migration-create-index-if-not-exists-test` - IF NOT EXISTS on indexes
- `migration-create-sequence-if-not-exists-test` - IF NOT EXISTS on sequences

Edge cases:
- `migration-modify-column-not-applied-test` - Column type changes not applied (expected)
- `migration-sql-statements-use-if-not-exists-test` - All SQL statements use IF NOT EXISTS pattern

---

## Documentation

### 15. ~~Update documentation~~ DONE

**Status:** DONE

Documentation added:
- **README.md** - Comprehensive project documentation with:
  - Quick start guide
  - Installation instructions
  - All attribute options with examples:
    - `::pg2/table` - Custom table names
    - `::pg2/column-name` - Custom column names
    - `::pg2/fk-attr` - Reverse FK for to-many relationships
    - `::pg2/max-length` - VARCHAR length limits
    - `::pg2/form->sql-value` - Custom write transformers
    - `::pg2/sql->form-value` - Custom read transformers
    - `::pg2/delete-orphan?` - Cascade delete for owned relationships
    - `::pg2/order-by` - Sorted collections
  - Supported types table
  - Relationship pattern examples (to-one, to-many, M:M, self-ref)
- **MIGRATION.md** - Already existed with comprehensive migration guide from fulcro-rad-sql

---

## Future Improvements (inspired by walkable)

Analysis of [walkable](https://github.com/walkable-server/walkable) revealed several architectural patterns that could improve pg2's performance and developer experience.

### 16. Floor-Plan Pre-Compilation

**Priority:** High
**Impact:** Performance improvement, better error messages at startup

Currently, pg2 builds SQL queries and compiles resolvers dynamically during `generate-resolvers`. Walkable uses a "floor-plan" architecture that compiles the attribute registry into optimized query templates once at startup.

**Current approach (pg2):**
```clojure
;; read.clj - queries built at resolver creation time
(defn generate-resolvers [attributes schema-name]
  ;; For each attribute, build resolver with SQL generation logic
  ;; SQL strings constructed per-query at runtime
  )
```

**Proposed approach:**
```clojure
;; Compile phase (once at startup)
(defn compile-floor-plan [attributes schema-name]
  ;; Pre-compute for each attribute:
  ;; - SQL query templates with parameter placeholders
  ;; - Column lists, join clauses, where templates
  ;; - Validation of attribute relationships
  ;; Returns optimized lookup structure
  )

;; Runtime phase (per-query)
(defn generate-resolvers [floor-plan]
  ;; Resolvers just fill in parameters to pre-compiled templates
  ;; Zero SQL string construction at query time
  )
```

**Benefits:**
- Faster query execution (no string building per-query)
- Validation errors surface at startup, not first query
- Easier to cache and optimize query plans
- Cleaner separation of concerns

**Implementation steps:**
1. Analyze current `generate-resolvers` to identify what's computed per-query vs could be pre-computed
2. Create `compile-floor-plan` function that builds lookup tables for:
   - Table/column mappings per attribute
   - SQL fragments for SELECT, FROM, JOIN, WHERE clauses
   - Batch query templates
3. Refactor resolvers to use pre-compiled floor-plan
4. Add startup validation (missing refs, circular dependencies, etc.)
5. Benchmark before/after

---

### 17. Join-Path Specification for Relationships

**Priority:** Medium
**Impact:** Cleaner M:M handling, more flexible relationship modeling

Walkable uses a `join-path` specification that elegantly handles 2-hop (direct FK), 4-hop (join table), and N-hop relationships with a single abstraction.

**Current approach (pg2):**
```clojure
;; M:M requires explicit join entity and two attributes
(defattr issue-labels :issue/labels :ref
  {::attr/cardinality :many
   ::attr/target :issue-label/id        ; Points to join entity
   ::pg2/fk-attr :issue-label/issue})

;; Then need another query to get actual labels from issue-labels
```

**Proposed approach:**
```clojure
;; Direct specification of the full path
(defattr issue-labels :issue/labels :ref
  {::attr/cardinality :many
   ::attr/target :label/id
   ::pg2/join-path [:issue/id :issue-label/issue  ; hop 1-2
                    :issue-label/label :label/id]  ; hop 3-4
   ::pg2/order-by :label/name})

;; 2-hop (standard FK) still works
(defattr issue-project :issue/project :ref
  {::attr/cardinality :one
   ::attr/target :project/id
   ::pg2/join-path [:issue/project-id :project/id]})  ; explicit, or inferred
```

**Benefits:**
- Single query for M:M instead of two resolver hops
- No need to expose join entities in the API
- Cleaner attribute definitions
- Supports arbitrary-depth relationships (e.g., grandchildren)

**Implementation steps:**
1. Add `::pg2/join-path` attribute option
2. Implement join-path parsing (validate hop count is even, endpoints match)
3. Generate appropriate SQL JOINs for multi-hop paths
4. For 4-hop paths, generate single query with JOIN through intermediate table
5. Maintain backward compatibility with `::pg2/fk-attr` (convert to join-path internally)
6. Add tests for 2-hop, 4-hop, and 6-hop paths

**SQL generation example for 4-hop:**
```sql
-- For :issue/labels with join-path [:issue/id :issue-label/issue :issue-label/label :label/id]
SELECT l.* FROM labels l
JOIN issue_labels il ON il.label = l.id
WHERE il.issue IN ($1, $2, $3)
ORDER BY l.name
```

---

### 18. Formula-Based Pseudo-Columns

**Priority:** Medium
**Impact:** Computed fields evaluated in SQL, not Clojure

Walkable supports "pseudo-columns" - computed fields defined as SQL expressions that can be used in SELECT, WHERE, and ORDER BY clauses.

**Current approach (pg2):**
```clojure
;; Computed values require Clojure post-processing
;; Cannot filter or sort by computed values in SQL
(defattr person-age :person/age :int
  {::attr/computed (fn [env person]
                     (- (year (now)) (:person/birth-year person)))})
```

**Proposed approach:**
```clojure
;; SQL-level computation
(defattr person-age :person/age :int
  {::pg2/formula [:- [:extract :year [:now]] :person/birth-year]
   ::pg2/pseudo-column? true})  ; Not stored, computed

;; Can now filter by age in SQL
(defattr adults :person/adults :ref
  {::pg2/filter [:>= :person/age 18]})  ; Uses pseudo-column

;; Aggregate pseudo-columns
(defattr project-issue-count :project/issue-count :int
  {::pg2/formula [:count :issue/id]
   ::pg2/pseudo-column? true})
```

**Benefits:**
- Filtering/sorting by computed values happens in SQL (faster)
- Reduces data transfer (compute in DB, not app)
- Enables SQL aggregates (COUNT, SUM, AVG) as virtual attributes
- Supports complex expressions (CASE, COALESCE, date math)

**Implementation steps:**
1. Define expression DSL (subset of HoneySQL or custom)
   - Arithmetic: `:+`, `:-`, `:*`, `:/`
   - Comparison: `:=`, `:<`, `:>`, `:<=`, `:>=`, `:<>`
   - Logical: `:and`, `:or`, `:not`
   - Functions: `:count`, `:sum`, `:avg`, `:max`, `:min`, `:coalesce`, `:case`
   - Date/time: `:now`, `:extract`, `:date-diff`
2. Add `::pg2/formula` attribute option
3. Compile formulas to SQL fragments during floor-plan compilation
4. Inject pseudo-columns into SELECT clause when requested
5. Allow pseudo-columns in `::pg2/filter` and `::pg2/order-by`
6. Handle dependencies (pseudo-column referencing another pseudo-column)

**SQL generation example:**
```sql
-- Query requesting :person/name and :person/age (pseudo-column)
SELECT
  p.name,
  (EXTRACT(YEAR FROM NOW()) - p.birth_year) AS age
FROM persons p
WHERE (EXTRACT(YEAR FROM NOW()) - p.birth_year) >= 18
ORDER BY age DESC
```

---

### 19. Enhanced Pagination with Validation

**Priority:** Low
**Impact:** Safer pagination, better defaults

Walkable has a sophisticated pagination system with validation and fallbacks.

**Current approach (pg2):**
```clojure
;; Basic order-by support
(defattr project-issues :project/issues :ref
  {::pg2/order-by :issue/created-at})
;; No limit/offset support in attributes
;; No validation of user-supplied order-by
```

**Proposed approach:**
```clojure
(defattr project-issues :project/issues :ref
  {::pg2/order-by :issue/created-at         ; Default order
   ::pg2/order-by-direction :desc           ; Default direction
   ::pg2/order-by-allowed #{:issue/created-at :issue/priority :issue/title}  ; Allowlist
   ::pg2/default-limit 50                   ; Default page size
   ::pg2/max-limit 1000                     ; Hard cap
   ::pg2/nulls-position :last})             ; NULLS FIRST/LAST
```

**Query-time override:**
```clojure
;; Client can request different pagination (validated against allowlist)
[{(:project/issues {:order-by [:issue/priority :desc]
                    :limit 20
                    :offset 40})
  [:issue/id :issue/title]}]
```

**Benefits:**
- Prevent arbitrary column ordering (SQL injection vector)
- Consistent defaults across the API
- Hard limits prevent accidental full-table fetches
- Explicit NULLS handling

**Implementation steps:**
1. Add pagination attribute options (`::pg2/default-limit`, `::pg2/max-limit`, etc.)
2. Add `::pg2/order-by-allowed` for column allowlisting
3. Parse pagination params from Pathom query params
4. Validate against allowlist, apply fallbacks
5. Generate LIMIT/OFFSET/ORDER BY clauses
6. Add NULLS FIRST/LAST support

---

### 20. Query-Time Filtering

**Priority:** Medium
**Impact:** Dynamic WHERE clauses from client queries

Allow clients to pass filter conditions at query time, validated against allowed columns.

**Current approach (pg2):**
```clojure
;; No support for dynamic filters
;; All filtering must be done in custom resolvers
```

**Proposed approach:**
```clojure
;; Attribute definition
(defattr project-issues :project/issues :ref
  {::pg2/filterable #{:issue/status :issue/priority :issue/type}})

;; Query with filter
[{(:project/issues {:filter [:and
                              [:= :issue/status :open]
                              [:> :issue/priority 3]]})
  [:issue/id :issue/title]}]
```

**Benefits:**
- Reduces need for custom resolvers
- Filters execute in SQL (efficient)
- Allowlist prevents arbitrary column access
- Composable with pagination

**Implementation steps:**
1. Add `::pg2/filterable` attribute option (set of allowed filter columns)
2. Define filter expression DSL (reuse from pseudo-columns)
3. Parse filter params from Pathom query params
4. Validate all referenced columns are in allowlist
5. Compile filter to WHERE clause
6. Combine with any static filters from attribute definition
