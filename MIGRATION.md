---
status: stable
contributions:
  - Add troubleshooting section for common migration issues
  - Add examples for custom resolver migration
  - Document error handling differences between versions
---

# Migration Guide: fulcro-rad-sql → fulcro-rad-pg2

This guide covers migrating from `fulcro-rad-sql` to `fulcro-rad-pg2`.

## Why fulcro-rad-pg2?

`fulcro-rad-pg2` is a fork of `fulcro-rad-sql` optimized specifically for PostgreSQL:

| Aspect | fulcro-rad-sql | fulcro-rad-pg2 |
|--------|----------------|----------------|
| **Databases** | PostgreSQL, H2, MariaDB, MySQL | PostgreSQL only |
| **Driver** | JDBC (next.jdbc) | pg2 (native) |
| **Connection Pool** | HikariCP | pg.pool |
| **Performance** | Good | 70-99% faster reads |
| **Flyway** | Built-in | Removed (use external) |

If you need H2/MariaDB/MySQL support, stay on `fulcro-rad-sql`.

## Breaking Changes Summary

1. **PostgreSQL only** - H2, MariaDB, and MySQL support removed
2. **pg2 driver** - Replaces next.jdbc + HikariCP
3. **Namespace change** - `database-adapters.sql` → `database-adapters.pg2`
4. **Attribute options renamed** - `ref` → `fk-attr`, `delete-referent?` → `delete-orphan?`
5. **Option removed** - `owns-ref?` is no longer used
6. **Connection pools** - pg.pool replaces HikariCP
7. **Flyway removed** - Use external migration tools

## Dependency Changes

### Old deps.edn (fulcro-rad-sql)

```clojure
{:deps {com.fulcrologic/fulcro-rad-sql {:mvn/version "1.x.x"}
        com.github.seancorfield/next.jdbc {:mvn/version "1.3.874"}
        com.zaxxer/HikariCP {:mvn/version "5.0.1"}
        org.flywaydb/flyway-core {:mvn/version "9.19.4"}}}
```

### New deps.edn (fulcro-rad-pg2)

```clojure
{:deps {yenda/fulcro-rad-pg2 {:mvn/version "0.1.0"}
        ;; pg2 is included as a dependency, but you may want to specify version
        com.github.igrishaev/pg2-core {:mvn/version "0.1.41"}}}
```

**Removed:**
- `next.jdbc` - replaced by pg2
- `HikariCP` - replaced by pg.pool
- `flyway-core`, `flyway-mysql` - removed entirely

## Database Support

| Database   | fulcro-rad-sql | fulcro-rad-pg2 |
|------------|----------------|----------------|
| PostgreSQL | ✅             | ✅             |
| H2         | ✅             | ❌             |
| MariaDB    | ✅             | ❌             |
| MySQL      | ✅             | ❌             |

If you need H2/MariaDB/MySQL support, stay on `fulcro-rad-sql`.

## Namespace Changes

All namespaces change from `sql` to `pg2`:

| Old (fulcro-rad-sql) | New (fulcro-rad-pg2) |
|----------------------|----------------------|
| `com.fulcrologic.rad.database-adapters.sql` | `com.fulcrologic.rad.database-adapters.pg2` |
| `com.fulcrologic.rad.database-adapters.sql.plugin` | `com.fulcrologic.rad.database-adapters.pg2.plugin` |
| `com.fulcrologic.rad.database-adapters.sql.read` | `com.fulcrologic.rad.database-adapters.pg2.read` |
| `com.fulcrologic.rad.database-adapters.sql.write` | `com.fulcrologic.rad.database-adapters.pg2.write` |
| `com.fulcrologic.rad.database-adapters.sql.pg2` | `com.fulcrologic.rad.database-adapters.pg2.driver` |

### Keyword Namespace Changes

```clojure
;; OLD (fulcro-rad-sql)
::rad.sql/table
::rad.sql/column-name
::rad.sql/fk-attr
::rad.sql/delete-orphan?

;; NEW (fulcro-rad-pg2)
::rad.pg2/table
::rad.pg2/column-name
::rad.pg2/fk-attr
::rad.pg2/delete-orphan?
```

## Attribute Option Changes

### `ref` → `fk-attr`

The `ref` option has been renamed to `fk-attr` for clarity. It answers the question: "Which attribute has the foreign key?"

```clojure
;; OLD (fulcro-rad-sql with old option name)
(defattr organization-domains :organization/domains :ref
  {::attr/cardinality :many
   ::attr/target :domain/id
   ::rad.sql/ref :domain/organization})

;; NEW (fulcro-rad-pg2)
(defattr organization-domains :organization/domains :ref
  {::attr/cardinality :many
   ::attr/target :domain/id
   ::rad.pg2/fk-attr :domain/organization})
```

### `delete-referent?` → `delete-orphan?`

The `delete-referent?` option has been renamed to `delete-orphan?` for clarity. It answers: "Should orphans be deleted?"

```clojure
;; OLD (fulcro-rad-sql with old option name)
(defattr document-metadata :document/metadata :ref
  {::attr/cardinality :one
   ::attr/target :metadata/id
   ::rad.sql/ref :metadata/document
   ::rad.sql/delete-referent? true})

;; NEW (fulcro-rad-pg2)
(defattr document-metadata :document/metadata :ref
  {::attr/cardinality :one
   ::attr/target :metadata/id
   ::rad.pg2/fk-attr :metadata/document
   ::rad.pg2/delete-orphan? true})
```

### `owns-ref?` - Removed

The `owns-ref?` option was vestigial and never read by the implementation. Remove it from your attributes.

## Connection Pool Configuration

### Old: HikariCP (fulcro-rad-sql)

```clojure
;; config.edn
{::sql/databases
 {:main {:hikaricp/config {"dataSourceClassName" "org.postgresql.ds.PGSimpleDataSource"
                           "dataSource.serverName" "localhost"
                           "dataSource.user" "myuser"
                           "dataSource.databaseName" "mydb"}
         :sql/schema :production}}}

;; startup code
(require '[com.fulcrologic.rad.database-adapters.sql.connection :as conn])
(conn/start-databases! config)
```

### New: pg.pool (fulcro-rad-pg2)

```clojure
;; config.edn - simpler configuration
{::pg2/databases
 {:main {:pg2/config {:host "localhost"
                      :port 5432
                      :user "myuser"
                      :password "mypass"
                      :database "mydb"}
         :pg2/schema :production}}}

;; startup code
(require '[com.fulcrologic.rad.database-adapters.pg2.driver :as driver])
(driver/create-pool! config)
```

**pg.pool options** (passed via `:pg2/config`):
- `:host`, `:port`, `:user`, `:password`, `:database` - connection info
- `:pool-min-size`, `:pool-max-size` - pool sizing
- `:pool-expire-threshold-ms` - connection lifetime

## Migration Tools

Flyway integration has been removed. Use external migration tools:

- **Flyway CLI** - Run migrations separately before app startup
- **Migratus** - Clojure migration library
- **Manual SQL** - For simple schemas

The auto-schema generation (`:pg2/auto-create-missing?`) is still available for development use.

## API Changes

### Removed namespaces

- `com.fulcrologic.rad.database-adapters.sql.vendor` - vendor adapter protocol
- `com.fulcrologic.rad.database-adapters.sql.result-set` - JDBC result set handling
- `com.fulcrologic.rad.database-adapters.sql.connection` - HikariCP connection management

### New namespaces

- `com.fulcrologic.rad.database-adapters.pg2.driver` - pg2 driver functions
- `com.fulcrologic.rad.database-adapters.pg2.read` - read/query logic
- `com.fulcrologic.rad.database-adapters.pg2.write` - save/mutation logic

### Query functions

```clojure
;; OLD: next.jdbc
(require '[next.jdbc.sql :as jdbc.sql])
(jdbc.sql/query conn ["SELECT * FROM users WHERE id = ?" id])

;; NEW: pg2
(require '[com.fulcrologic.rad.database-adapters.pg2.driver :as driver])
(driver/pg2-query! pool "SELECT * FROM users WHERE id = $1" [id])
```

Note: pg2 uses `$1`, `$2` parameter placeholders instead of `?`.

## Error Handling

Error conditions are now mapped from PostgreSQL error codes:

```clojure
(require '[com.fulcrologic.rad.database-adapters.pg2.write :as write])

(try
  (write/save-form! env params)
  (catch Exception e
    (let [data (ex-data e)]
      (case (:cause data)
        ::write/unique-violation (handle-duplicate)
        ::write/not-null-violation (handle-missing-field)
        ::write/serialization-failure (handle-retry)
        (throw e)))))
```

## Full Migration Checklist

1. [ ] Update `deps.edn` - remove fulcro-rad-sql and JDBC deps, add fulcro-rad-pg2
2. [ ] Ensure PostgreSQL is your database (no H2/MariaDB/MySQL)
3. [ ] Search/replace namespace: `database-adapters.sql` → `database-adapters.pg2`
4. [ ] Search/replace keyword namespace: `rad.sql/` → `rad.pg2/`
5. [ ] Search/replace `::rad.sql/ref` → `::rad.pg2/fk-attr` (if using old option name)
6. [ ] Search/replace `::rad.sql/delete-referent?` → `::rad.pg2/delete-orphan?` (if using old option name)
7. [ ] Remove all `::rad.sql/owns-ref?` options
8. [ ] Update connection pool configuration for pg.pool
9. [ ] Remove Flyway configuration, use external migrations
10. [ ] Update any direct query code to use pg2 parameter syntax (`$1` vs `?`)
11. [ ] Test thoroughly - the pg2 driver has different behavior for some edge cases

## Sed Commands for Bulk Updates

```bash
# Namespace change (both clj require statements and file paths)
find . -name "*.clj" -o -name "*.cljc" | xargs sed -i 's/database-adapters\.sql/database-adapters.pg2/g'

# Keyword namespace change
find . -name "*.clj" -o -name "*.cljc" | xargs sed -i 's/rad\.sql/rad.pg2/g'

# Rename ref to fk-attr (if coming from old fulcro-rad-sql with ref option)
find . -name "*.clj" -o -name "*.cljc" | xargs sed -i 's/::rad\.pg2\/ref/::rad.pg2\/fk-attr/g'

# Rename delete-referent? to delete-orphan? (if using old option name)
find . -name "*.clj" -o -name "*.cljc" | xargs sed -i 's/::rad\.pg2\/delete-referent?/::rad.pg2\/delete-orphan?/g'

# Remove owns-ref? lines (manual review recommended)
grep -r "owns-ref?" --include="*.clj" --include="*.cljc" .
```
