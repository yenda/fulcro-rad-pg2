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

### 8. Transformer edge cases

**Priority:** Low

Custom transformer functions may receive edge case inputs:
- `nil` values
- Empty strings
- Empty collections

**Test cases needed:**
```clojure
(deftest transformer-nil-handling-test
  ;; Verify transformers handle nil gracefully
  )

(deftest transformer-empty-collection-test
  ;; Verify CSV transformer handles empty vector
  )
```

---

## Performance Tests

The `perf/` directory contains read benchmarks. Consider adding:

- Write benchmarks (single insert, batch insert)
- Update benchmarks
- Delete benchmarks (with cascade)
- Mixed workload benchmarks

---

## Documentation

- Document the `sql->form-value` limitation until fixed
- Add migration guide from fulcro-rad-sql
- Document all supported attribute options with examples
