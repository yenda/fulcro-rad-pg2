# Migration Guide: fulcro-rad-sql 1.x → 2.0

This guide covers migrating from the JDBC-based multi-database version to the pg2-based PostgreSQL-only version.

## Breaking Changes Summary

1. **PostgreSQL only** - H2, MariaDB, and MySQL support removed
2. **pg2 driver** - Replaces next.jdbc + HikariCP
3. **Attribute options renamed** - `ref` → `fk-attr`, `delete-referent?` → `delete-orphan?`
4. **Option removed** - `owns-ref?` is no longer used
5. **Connection pools** - pg.pool replaces HikariCP
6. **Flyway removed** - Use external migration tools

## Dependency Changes

### Old deps.edn

```clojure
{:deps {com.github.seancorfield/next.jdbc {:mvn/version "1.3.874"}
        com.zaxxer/HikariCP {:mvn/version "5.0.1"}
        org.flywaydb/flyway-core {:mvn/version "9.19.4"}
        org.flywaydb/flyway-mysql {:mvn/version "9.19.4"}}}
```

### New deps.edn

```clojure
{:deps {com.github.igrishaev/pg2-core {:mvn/version "0.1.41"}
        org.postgresql/postgresql {:mvn/version "42.6.0"}  ; for exception classes
        metosin/malli {:mvn/version "0.20.0"}}}
```

**Removed:**
- `next.jdbc` - replaced by pg2
- `HikariCP` - replaced by pg.pool
- `flyway-core`, `flyway-mysql` - removed entirely

**Added:**
- `pg2-core` - PostgreSQL driver with connection pooling
- `malli` - schema validation

## Database Support

| Database   | Old Version | New Version |
|------------|-------------|-------------|
| PostgreSQL | ✅          | ✅          |
| H2         | ✅          | ❌          |
| MariaDB    | ✅          | ❌          |
| MySQL      | ✅          | ❌          |

If you need H2/MariaDB/MySQL support, stay on version 1.x.

## Attribute Option Changes

### `ref` → `fk-attr`

The `ref` option has been renamed to `fk-attr` for clarity. It answers the question: "Which attribute has the foreign key?"

```clojure
;; OLD
(defattr organization-domains :organization/domains :ref
  {::attr/cardinality :many
   ::attr/target :domain/id
   ::rad.sql/ref :domain/organization})  ; OLD

;; NEW
(defattr organization-domains :organization/domains :ref
  {::attr/cardinality :many
   ::attr/target :domain/id
   ::rad.sql/fk-attr :domain/organization})  ; NEW
```

### `delete-referent?` → `delete-orphan?`

The `delete-referent?` option has been renamed to `delete-orphan?` for clarity. It answers: "Should orphans be deleted?"

```clojure
;; OLD
(defattr document-metadata :document/metadata :ref
  {::attr/cardinality :one
   ::attr/target :metadata/id
   ::rad.sql/ref :metadata/document
   ::rad.sql/delete-referent? true})  ; OLD

;; NEW
(defattr document-metadata :document/metadata :ref
  {::attr/cardinality :one
   ::attr/target :metadata/id
   ::rad.sql/fk-attr :metadata/document
   ::rad.sql/delete-orphan? true})  ; NEW
```

### `owns-ref?` - Removed

The `owns-ref?` option was vestigial and never read by the implementation. Remove it from your attributes.

```clojure
;; OLD - remove this option
(defattr account-address :account/address :ref
  {::attr/cardinality :one
   ::attr/target :address/id
   ::rad.sql/owns-ref? true})  ; REMOVE THIS

;; NEW - just remove it, it was never used
(defattr account-address :account/address :ref
  {::attr/cardinality :one
   ::attr/target :address/id})
```

## Connection Pool Configuration

### Old: HikariCP

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
(conn/start-databases! config)  ; returns {schema-key pool}
```

### New: pg.pool

```clojure
;; config.edn - simpler configuration
{::sql/databases
 {:main {:pg/config {:host "localhost"
                     :port 5432
                     :user "myuser"
                     :password "mypass"
                     :database "mydb"}
         :sql/schema :production}}}

;; startup code
(require '[com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2])
(pg2/create-pool! config)  ; returns pg.pool instance
```

**pg.pool options** (passed via `:pg/config`):
- `:host`, `:port`, `:user`, `:password`, `:database` - connection info
- `:pool-min-size`, `:pool-max-size` - pool sizing
- `:pool-expire-threshold-ms` - connection lifetime

## Migration Tools

Flyway integration has been removed. Use external migration tools:

- **Flyway CLI** - Run migrations separately before app startup
- **Migratus** - Clojure migration library
- **Manual SQL** - For simple schemas

The auto-schema generation (`:sql/auto-create-missing?`) is still available for development use.

## Plugin Configuration

### Old: Vendor adapters

```clojure
(require '[com.fulcrologic.rad.database-adapters.sql.plugin :as sql.plugin])

;; wrap-env selected vendor adapter based on :sql/vendor
{::sql/databases
 {:main {:sql/vendor :postgresql  ; or :h2, :mariadb
         :sql/schema :production
         ...}}}
```

### New: PostgreSQL only

```clojure
(require '[com.fulcrologic.rad.database-adapters.sql.plugin :as sql.plugin])

;; No vendor selection needed - always PostgreSQL
{::sql/databases
 {:main {:sql/schema :production
         ...}}}
```

## API Changes

### Removed namespaces

- `com.fulcrologic.rad.database-adapters.sql.vendor` - vendor adapter protocol
- `com.fulcrologic.rad.database-adapters.sql.result-set` - JDBC result set handling
- `com.fulcrologic.rad.database-adapters.sql.connection` - HikariCP connection management

### New namespaces

- `com.fulcrologic.rad.database-adapters.sql.pg2` - pg2 driver functions
- `com.fulcrologic.rad.database-adapters.sql.read` - read/query logic
- `com.fulcrologic.rad.database-adapters.sql.write` - save/mutation logic

### Query functions

```clojure
;; OLD: next.jdbc
(require '[next.jdbc.sql :as jdbc.sql])
(jdbc.sql/query conn ["SELECT * FROM users WHERE id = ?" id])

;; NEW: pg2
(require '[com.fulcrologic.rad.database-adapters.sql.pg2 :as pg2])
(pg2/pg2-query! pool "SELECT * FROM users WHERE id = $1" [id])
```

Note: pg2 uses `$1`, `$2` parameter placeholders instead of `?`.

## Error Handling

Error conditions are now mapped from PostgreSQL error codes:

```clojure
(require '[com.fulcrologic.rad.database-adapters.sql.write :as write])

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

1. [ ] Update `deps.edn` - remove JDBC deps, add pg2
2. [ ] Ensure PostgreSQL is your database (no H2/MariaDB/MySQL)
3. [ ] Search/replace `::rad.sql/ref` → `::rad.sql/fk-attr`
4. [ ] Search/replace `::rad.sql/delete-referent?` → `::rad.sql/delete-orphan?`
5. [ ] Remove all `::rad.sql/owns-ref?` options
6. [ ] Update connection pool configuration for pg.pool
7. [ ] Remove Flyway configuration, use external migrations
8. [ ] Update any direct query code to use pg2 parameter syntax (`$1` vs `?`)
9. [ ] Test thoroughly - the pg2 driver has different behavior for some edge cases

## Sed Commands for Bulk Updates

```bash
# Rename ref to fk-attr
find . -name "*.clj" -o -name "*.cljc" | xargs sed -i 's/::rad\.sql\/ref/::rad.sql\/fk-attr/g'

# Rename delete-referent? to delete-orphan?
find . -name "*.clj" -o -name "*.cljc" | xargs sed -i 's/::rad\.sql\/delete-referent?/::rad.sql\/delete-orphan?/g'

# Remove owns-ref? lines (manual review recommended)
grep -r "owns-ref?" --include="*.clj" --include="*.cljc" .
```
