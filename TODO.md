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

### 3. Update operations

**Priority:** High

No tests verify that updating existing entities works correctly:
- Changing scalar values
- Changing ref values (re-parenting)
- Partial updates (only some fields)

**Test cases needed:**
```clojure
(deftest update-scalar-values-test
  ;; Change issue title, status, priority
  )

(deftest update-ref-values-test
  ;; Move issue to different project
  ;; Change issue reporter
  )
```

---

### 4. Null/nil handling

**Priority:** Medium

No tests for setting attributes to `nil`:
- Optional attributes set to nil
- Clearing a ref (setting to-one ref to nil)
- Behavior with required vs optional fields

**Test cases needed:**
```clojure
(deftest set-optional-to-nil-test
  ;; Set issue/description to nil
  )

(deftest clear-to-one-ref-test
  ;; Set issue/milestone to nil
  )
```

---

### 5. Batch resolver behavior

**Priority:** Medium

Resolvers are configured with `batch? true` but no tests verify batching works correctly:
- Multiple entities resolved in single query
- Performance characteristics
- Correct data association

**Test cases needed:**
```clojure
(deftest batch-resolution-test
  ;; Query 50 issues, verify single SQL query executed
  ;; Verify each issue gets correct data
  )
```

---

### 6. Error scenarios

**Priority:** Medium

No tests for database constraint violations:
- Foreign key violations (referencing non-existent entity)
- Unique constraint violations (duplicate email)
- Not-null constraint violations
- Data type mismatches

**Test cases needed:**
```clojure
(deftest fk-violation-test
  ;; Try to create issue with non-existent project
  )

(deftest unique-violation-test
  ;; Try to create two users with same email
  )
```

---

### 7. `order-by` edge cases

**Priority:** Low

Basic ordering is tested, but edge cases are not:
- Null values in ordered column
- Duplicate values (stability)
- Descending order (if supported)

**Test cases needed:**
```clojure
(deftest order-by-with-nulls-test
  ;; Labels with nil position should come last
  )
```

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
