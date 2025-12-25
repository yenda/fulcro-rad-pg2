(ns com.fulcrologic.rad.database-adapters.pg2.migration
  "pg2-based migration runner for PostgreSQL.

   Replaces Flyway with a simpler migration system that:
   - Tracks applied migrations in a schema_migrations table
   - Executes SQL files via pg2
   - Supports auto-schema generation from RAD attributes"
  (:require
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [com.fulcrologic.guardrails.core :refer [>defn =>]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.pg2 :as rad.pg2]
   [com.fulcrologic.rad.database-adapters.pg2.driver :as pg2]
   [com.fulcrologic.rad.database-adapters.pg2.schema :as sql.schema]
   [pg.core :as pg]
   [pg.pool :as pg.pool]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Type Mapping

(def type-map
  {:string "VARCHAR(2048)"
   :password "VARCHAR(512)"
   :boolean "BOOLEAN"
   :int "INTEGER"
   :long "BIGINT"
   :decimal "decimal(20,2)"
   :instant "TIMESTAMP WITH TIME ZONE"
   ;; There is no standard SQL enum, and many ppl think they are a bad idea in general. Given
   ;; that we have other ways of enforcing them we use a standard type instead.
   :enum "VARCHAR(200)"
   :keyword "VARCHAR(200)"
   :symbol "VARCHAR(200)"
   :uuid "UUID"})

(>defn sql-type [{::attr/keys [type]
                  ::rad.pg2/keys [max-length]}]
       [::attr/attribute => string?]
       (if (#{:string :password :keyword :symbol} type)
         (if max-length
           (str "VARCHAR(" max-length ")")
           "VARCHAR(200)")
         (if-let [result (get type-map type)]
           result
           (do
             (log/error "Unsupported type" type)
             "TEXT"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema Operations

(>defn new-table [table]
       [string? => map?]
       {:type :table :table table})

(>defn new-scalar-column
       [table column attr]
       [string? string? ::attr/attribute => map?]
       {:type :column :table table :column column :attr attr})

(>defn new-id-column [table column attr]
       [string? string? ::attr/attribute => map?]
       {:type :id :table table :column column :attr attr})

(>defn new-ref-column [table column attr]
       [string? string? ::attr/attribute => map?]
       {:type :ref :table table :column column :attr attr})

(defmulti op->sql (fn [_k->attr _adapter {:keys [type]}] type))

(defmethod op->sql :table [_ _adapter {:keys [table]}] (format "CREATE TABLE IF NOT EXISTS %s ();\n" table))

(defmethod op->sql :ref [k->attr _ {:keys [table column attr]}]
  (let [{::attr/keys [cardinality target identities qualified-key]} attr
        target-attr (k->attr target)]
    (if (= :many cardinality)
      (do
        (when (not= 1 (count identities))
          (throw (ex-info "Reference column must have exactly 1 ::attr/identities entry." {:k qualified-key})))
        (enc/if-let [reverse-target-attr (k->attr (first identities))
                     rev-target-table (sql.schema/table-name k->attr reverse-target-attr)
                     rev-target-column (sql.schema/column-name reverse-target-attr)
                     table (sql.schema/table-name k->attr target-attr)
                     column (sql.schema/column-name k->attr attr)
                     index-name (str column "_idx")]
          (str
           (pg2/add-referential-column-statement
            table column (sql-type reverse-target-attr) rev-target-table rev-target-column)
           (format "CREATE INDEX IF NOT EXISTS %s ON %s(%s);\n"
                   index-name table column))
          (throw (ex-info "Cannot create to-many reference column." {:k qualified-key}))))
      (enc/if-let [origin-table (sql.schema/table-name k->attr attr)
                   origin-column (sql.schema/column-name attr)
                   target-table (sql.schema/table-name k->attr target-attr)
                   target-column (sql.schema/column-name target-attr)
                   target-type (sql-type target-attr)
                   index-name (str column "_idx")]
        (str
         (pg2/add-referential-column-statement
          origin-table origin-column target-type target-table target-column)
         (format "CREATE INDEX IF NOT EXISTS %s ON %s(%s);\n"
                 index-name table column))
        (throw (ex-info "Cannot create to-many reference column." {:k qualified-key}))))))

(defmethod op->sql :id [_k->attr _adapter {:keys [table column attr]}]
  (let [{::attr/keys [type]} attr
        index-name (str table "_" column "_idx")
        sequence-name (str table "_" column "_seq")
        typ (sql-type attr)]
    (str
     (if (#{:int :long} type)
       (str
        (format "CREATE SEQUENCE IF NOT EXISTS %s;\n" sequence-name)
        (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s DEFAULT nextval('%s');\n"
                table column typ sequence-name))
       (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;\n"
               table column typ sequence-name))
     (format "CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(%s);\n"
             index-name table column))))

(defmethod op->sql :column [_key->attr _adapter {:keys [table column attr]}]
  (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;\n" table column (sql-type attr)))

(defn attr->ops [schema-name key->attribute {::attr/keys [_qualified-key type identity? _identities]
                                             :keys [::attr/schema]
                                             :as attr}]
  (when (= schema schema-name)
    (enc/if-let [tables-and-columns (seq (sql.schema/tables-and-columns key->attribute attr))]
      (reduce
       (fn [s [table col]]
         (-> s
             (conj (new-table table))
             (conj (cond
                     identity? (new-id-column table col attr)
                     (= :ref type) (new-ref-column table col attr)
                     :else (new-scalar-column table col attr)))))
       []
       tables-and-columns)
      (log/error "Correct schema for attribute, but generation failed: "
                 (::attr/qualified-key attr)
                 (when (nil? (sql-type attr))
                   (str " (No mapping for type " type ")"))))))

(>defn automatic-schema
       "Returns SQL schema for all attributes that support it."
       [schema-name attributes]
       [keyword? ::attr/attributes => (s/coll-of string?)]
       (let [key->attribute (attr/attribute-map attributes)
             db-ops (mapcat (partial attr->ops schema-name key->attribute) attributes)
             {:keys [id table column ref]} (group-by :type db-ops)
             op (partial op->sql key->attribute nil)
             new-tables (mapv op (set table))
             new-ids (mapv op (set id))
             new-columns (mapv op (set column))
             new-refs (mapv op (set ref))]
         (vec (concat new-tables new-ids new-columns new-refs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; pg2-based Migration Runner

(defn- ensure-migrations-table!
  "Create the schema_migrations table if it doesn't exist."
  [conn]
  (pg/execute conn
              "CREATE TABLE IF NOT EXISTS schema_migrations (
                 version VARCHAR(255) PRIMARY KEY,
                 applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
               )"))

(defn- get-applied-migrations
  "Get set of already applied migration versions."
  [conn]
  (let [rows (pg/execute conn "SELECT version FROM schema_migrations")]
    (into #{} (map :version) rows)))

(defn- record-migration!
  "Record that a migration has been applied."
  [conn version]
  (pg/execute conn
              "INSERT INTO schema_migrations (version) VALUES ($1)"
              {:params [version]}))

(defn- parse-migration-version
  "Extract version from migration filename (e.g., 'V001__create_users.sql' -> 'V001')."
  [filename]
  (when-let [[_ version] (re-matches #"(V\d+)__.*\.sql" filename)]
    version))

(defn- find-migration-files
  "Find SQL migration files in the given locations.
   Locations can be:
   - classpath:path/to/migrations (searches classpath)
   - filesystem:/path/to/migrations (searches filesystem)
   - path/to/migrations (searches classpath by default)"
  [locations]
  (mapcat
   (fn [location]
     (let [[type path] (if (str/includes? location ":")
                         (str/split location #":" 2)
                         ["classpath" location])]
       (case type
         "classpath"
         (when-let [url (io/resource path)]
           (let [files (->> (io/file url)
                            file-seq
                            (filter #(.isFile %))
                            (filter #(str/ends-with? (.getName %) ".sql")))]
             (map (fn [f]
                    {:name (.getName f)
                     :version (parse-migration-version (.getName f))
                     :content (slurp f)})
                  files)))

         "filesystem"
         (let [dir (io/file path)]
           (when (.isDirectory dir)
             (let [files (->> (file-seq dir)
                              (filter #(.isFile %))
                              (filter #(str/ends-with? (.getName %) ".sql")))]
               (map (fn [f]
                      {:name (.getName f)
                       :version (parse-migration-version (.getName f))
                       :content (slurp f)})
                    files)))))))
   locations))

(defn- run-migrations!
  "Run pending migrations in version order."
  [pool migrations-config]
  (let [{:keys [locations schema]} migrations-config
        migrations (->> (find-migration-files locations)
                        (filter :version)
                        (sort-by :version))]
    (pg.pool/with-conn [conn pool]
      ;; Set search path if schema specified
      (when (and schema (not= schema "public"))
        (pg/execute conn (format "SET search_path TO %s, public" schema)))

      (ensure-migrations-table! conn)
      (let [applied (get-applied-migrations conn)]
        (doseq [{:keys [name version content]} migrations]
          (when-not (applied version)
            (log/info "Applying migration:" name)
            (pg/with-transaction [tx conn]
              ;; Execute the migration SQL
              (pg/execute tx content)
              ;; Record it as applied
              (record-migration! tx version))
            (log/info "Applied migration:" name)))))))

(defn- run-auto-schema!
  "Auto-create schema from RAD attributes using pg2."
  [pool schema all-attributes]
  (let [stmts (automatic-schema schema all-attributes)]
    (log/info "Automatically trying to create SQL schema from attributes.")
    (pg.pool/with-conn [conn pool]
      (doseq [s stmts]
        (try
          (pg/execute conn s)
          (catch Exception e
            (log/error e s)
            (throw e)))))))

(defn migrate!
  "Run migrations for all configured databases.

   Configuration options per database:
   - :sql/schema - The RAD schema name (keyword)
   - :sql/auto-create-missing? - Auto-create tables from attributes (boolean)
   - :migrations/locations - Vector of migration file locations (strings)
   - :migrations/enabled? - Whether to run migrations (boolean, default true if locations provided)"
  [config all-attributes connection-pools]
  (let [database-map (some-> config ::rad.pg2/databases)]
    (doseq [[dbkey dbconfig] database-map]
      (let [{:sql/keys [auto-create-missing? schema]
             :migrations/keys [locations enabled?]} dbconfig
            pool (get connection-pools dbkey)
            run-migrations? (and (seq locations)
                                 (not (false? enabled?)))]
        (cond
          (nil? pool)
          (log/error (str "No pool for " dbkey ". Skipping migrations."))

          run-migrations?
          (do
            (log/info (str "Running pg2 migrations for " dbkey))
            (run-migrations! pool {:locations locations
                                   :schema (or (:migrations/schema dbconfig) "public")}))

          auto-create-missing?
          (run-auto-schema! pool schema all-attributes))))))
