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

### 10. Self-referential relationships

**Priority:** Medium

Test hierarchical/recursive relationships:
- Parent/child issues (`issue/parent` → `issue/children`)
- Query parent from child
- Query children from parent
- Multiple levels deep

### 11. Many-to-many through join table

**Priority:** Low

Test many-to-many patterns:
- Issue ↔ Label through IssueLabel
- Query labels for issue
- Query issues for label
- Add/remove associations

---

## Automigration Testing

### 12. Schema introspection tests

**Priority:** Medium

Verify generated schema matches expectations by querying `pg_catalog`:
- Column types match `sql-type` output (e.g., `VARCHAR(200)`, `UUID`, `BIGINT`)
- Sequences created for int/long identity columns
- Indexes created for identity columns
- Foreign key references exist

### 13. Constraint verification tests

**Priority:** Low

Document and test constraint behavior:
- Verify which constraints ARE created (FK, unique index on id)
- Verify which constraints are NOT created (NOT NULL, UNIQUE on non-id)
- Test `::pg2/max-length` produces correct `VARCHAR(n)`

### 14. Migration diff testing

**Priority:** Low

Test incremental schema updates:
- Existing schema + new attribute → generates ALTER TABLE ADD COLUMN
- Existing schema + modified attribute → correct behavior (or error)
- Idempotency: running same migration twice is safe (IF NOT EXISTS)

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
