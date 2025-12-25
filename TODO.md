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

### 15. Update documentation

**Priority:** Medium

- ~~Document the `sql->form-value` limitation~~ (now fixed)
- Add migration guide from fulcro-rad-sql
- Document all supported attribute options with examples:
  - `::pg2/table`
  - `::pg2/column-name`
  - `::pg2/fk-attr`
  - `::pg2/max-length`
  - `::pg2/form->sql-value`
  - `::pg2/sql->form-value`
  - `::pg2/delete-orphan?`
  - `::pg2/order-by`
