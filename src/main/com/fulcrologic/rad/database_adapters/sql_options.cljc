(ns com.fulcrologic.rad.database-adapters.sql-options
  "Options supported by the SQL adapter")

(def table
  "Attribute option. The name of the database table. Use on `ao/identity? true` attributes.
   Defaults to the snake_case namespace name of the attribute."
  :com.fulcrologic.rad.database-adapters.sql/table)

(def column-name
  "Attribute option. The string name to use for the SQL column name. Defaults to
  the snake_case name of the attribute."
  :com.fulcrologic.rad.database-adapters.sql/column-name)

(def fk-attr
  "Which attribute has the foreign key?

   Attribute option. Indicates this is a reverse/virtual reference.

   The value is the qualified keyword of the attribute that stores the actual FK.
   When this attribute is modified, changes are propagated to the FK-owning attribute.

   FK ownership rules:
   - Attribute WITHOUT `fk-attr` -> stores FK directly in its table
   - Attribute WITH `fk-attr` -> reverse lookup, FK is stored by the referenced attr

   Example (one-to-one, child owns FK):
   ```clojure
   ;; Parent side - virtual ref, FK is stored by :language-course/course
   (defattr course-language-course :course/language-course :ref
     {ao/target :language-course/id
      so/fk-attr :language-course/course   ; Points to FK-owning attr
      so/delete-orphan? true})             ; Works because this attr has `fk-attr`

   ;; Child side - owns the FK (no `fk-attr`)
   (defattr language-course-course :language-course/course :ref
     {ao/target :course/id
      so/column-name \"course_id\"})    ; The actual FK column
   ```

   See `delete-orphan?` for important limitations when using these options together."
  :com.fulcrologic.rad.database-adapters.sql/fk-attr)

(def max-length
  "Attribute option. The max length for attributes that are internally represented by strings. This
   includes keywords, symbols, password, enumerations, and raw strings. This is ONLY used when this adapter
   generates schema for it. Only used by the auto-generation. Defaults to 2k for strings, and roughly
   200 characters for most other types."
  :com.fulcrologic.rad.database-adapters.sql/max-length)

(def ^:deprecated sql->form-value "DEPRECATED. See sql->model-value."
  :com.fulcrologic.rad.database-adapters.sql/sql->form-value)

(def ^:deprecated form->sql-value "DEPRECATED. See model->sql-value."
  :com.fulcrologic.rad.database-adapters.sql/form->sql-value)

(def model->sql-value
  "Attribute option. A `(fn [clj-value] sql-value)`. When defined, the
  writes via the plugin's form save this function will call this to convert the model value into
  something acceptable to the low-level JDBC call (see `next.jdbc.sql/execute!`).

  WARNING: The keyword name of this option does not match this option name.
  is :com.fulcrologic.rad.database-adapters.sql/form->sql-value."
  :com.fulcrologic.rad.database-adapters.sql/form->sql-value)

(def sql->model-value
  "Attribute option. A `(fn [sql-value] clojure-type)`. If defined, then this
   is called when reading raw results from the database, and can be used to convert
   the database value into the correct clojure data.

   WARNING: The keyword name of this option does not match this option name.
   is :com.fulcrologic.rad.database-adapters.sql/sql->form-value."
  :com.fulcrologic.rad.database-adapters.sql/sql->form-value)

(def connection-pools
  "Env key. This is the key under which your database connection(s) will appear
   in your Pathom resolvers when you use the pathom-plugin (see that docstring).

   This is actually a value that you generate, since when you install the pathom
   plugin you must provide it with a database mapping function. This key is
   where that database map is placed in the env."
  :com.fulcrologic.rad.database-adapters.sql/connection-pools)

(def databases
  "Config file key. Defines the databases used by the application in the config files and
   the resulting config object.

   The value of this option is a map from a developer-selected name (e.g. a shard or instance
   name) to a specification for a database/schema. The *keys* of the database are *not*
   schema names (from attributes), they are meant to allow you to create more than one
   instance of a database (potentially with the same schema) where some of your users
   might be on one, and others on another.

   The values in each database config can include any keys you want, but there are some
   predefined ones used by the built-in adapter:

   * `:pg2/config` - pg2 connection config (host, port, user, password, database)
   * `:pg2/pool` - pg2 pool options (pool-min-size, pool-max-size)
   * `:sql/auto-create-missing?` - When true, the adapter will try to generate schema for
     defined but missing attributes. NOT recommended for production use.
   * `:sql/schema` - The RAD schema (you define) that this database should use. Any attribute
     with a declared `ao/schema` that matches this should appear in the schema of this database.

   For example:

   ```
   :com.fulcrologic.rad.database-adapters.sql/databases
     {:main {:pg2/config {:host \"localhost\"
                          :port 5432
                          :user \"myuser\"
                          :password \"mypassword\"
                          :database \"mydb\"}
             :pg2/pool {:pool-min-size 2
                        :pool-max-size 10}
             :sql/auto-create-missing? false
             :sql/schema :production}}
   ```
   "
  :com.fulcrologic.rad.database-adapters.sql/databases)

(def delete-orphan?
  "Should orphans be deleted?

   Attribute option. Only has meaning for :ref types (both cardinalities) that also
   have the `fk-attr` option set.

   Semantic meaning: 'This child entity has no meaning as an orphan. If I stop
   referencing it, delete it.'

   When a reference is REMOVED (set to nil or removed from a collection), if this
   option is true, the entity that was being referenced will be deleted.

   This is different from SQL CASCADE:
   - CASCADE: Deletes children when PARENT is deleted
   - delete-orphan?: Deletes child when REFERENCE is removed (parent still exists)

   Example:
   ```clojure
   (defattr domains :organization/domains :ref
     {ao/target :organization.domain/id
      ao/cardinality :many
      so/fk-attr :organization.domain/organization
      so/delete-orphan? true})
   ```

   When you remove a domain from the organization's domains list, that domain
   entity is deleted. The domain has no meaning without its parent organization.

   ## Important Limitations

   **Single-level only:** delete-orphan? only deletes the immediate orphan. If the
   orphaned entity has its own children, they are NOT automatically deleted by the
   application. Use SQL CASCADE constraints (`ON DELETE CASCADE`) on your FK columns
   to handle nested cleanup.

   **Re-parenting:** To move a child from one parent to another, update the child's
   FK attribute directly rather than manipulating parent collections. If you remove
   from ParentA's collection and add to ParentB's collection in the same delta,
   delete-orphan? will trigger on removal and delete the child before the add.

   Correct re-parenting pattern:
   ```clojure
   ;; DO: Update FK directly
   {[:child/id child-id]
    {:child/parent {:before [:parent/id parent-a]
                    :after [:parent/id parent-b]}}}

   ;; DON'T: Manipulate collections (triggers delete-orphan?)
   {[:parent/id parent-a] {:parent/children {:before [...child...] :after []}}
    [:parent/id parent-b] {:parent/children {:after [...child...]}}}
   ```"
  :com.fulcrologic.rad.database-adapters.sql/delete-orphan?)

(def order-by
  "Attribute option. Specifies the attribute to use for ordering results in to-many
   collections. Only meaningful on :ref attributes with :many cardinality that also
   have `fk-attr` set.

   The value should be a qualified keyword of an attribute on the target entity.
   The generated SQL will include an ORDER BY clause inside the array_agg() function.

   Example:
   ```clojure
   (defattr category-children :category/children :ref
     {ao/cardinality :many
      ao/target :category/id
      so/fk-attr :category/parent
      so/order-by :category/position})  ; Order children by position
   ```

   Notes:
   - Uses PostgreSQL's default NULL ordering (NULLs last in ASC)
   - Duplicate values maintain stable order within the group
   - Only affects to-many resolvers, not direct queries"
  :com.fulcrologic.rad.database-adapters.sql/order-by)
