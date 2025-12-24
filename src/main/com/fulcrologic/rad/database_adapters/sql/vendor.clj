(ns com.fulcrologic.rad.database-adapters.sql.vendor
  (:require
   [clojure.spec.alpha :as s]
   [next.jdbc :as jdbc]))

(defprotocol VendorAdapter
  (relax-constraints-with-fn! [this datasource execute-fn]
    "Defer constraint checking until end of txn using provided execute-fn.")
  (add-referential-column-statement [this origin-table origin-column target-type target-table target-column]
    "Alter table and add a FK column."))

(defn relax-constraints!
  "Defer constraint checking until end of txn.
   Uses jdbc/execute! by default, or pass custom execute-fn for pg2."
  ([adapter datasource]
   (relax-constraints-with-fn! adapter datasource jdbc/execute!))
  ([adapter datasource execute-fn]
   (relax-constraints-with-fn! adapter datasource execute-fn)))

(s/def ::adapter (s/with-gen #(satisfies? VendorAdapter %) #(s/gen #{(reify VendorAdapter)})))

(deftype H2Adapter []
  VendorAdapter
  (relax-constraints-with-fn! [_ ds execute-fn] (execute-fn ds ["SET REFERENTIAL_INTEGRITY FALSE"]))
  (add-referential-column-statement [_ origin-table origin-column target-type target-table target-column]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s);\n"
            origin-table origin-column target-type target-table target-column)))

(deftype MariaDBAdapter []
  VendorAdapter
  (relax-constraints-with-fn! [_ ds execute-fn] (execute-fn ds ["SET FOREIGN_KEY_CHECKS = 0;"]))
  (add-referential-column-statement [_ origin-table origin-column target-type target-table target-column]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s);\n"
            origin-table origin-column target-type target-table target-column)))

(deftype PostgreSQLAdapter []
  VendorAdapter
  (relax-constraints-with-fn! [_ ds execute-fn] (execute-fn ds ["SET CONSTRAINTS ALL DEFERRED"]))
  (add-referential-column-statement [_ origin-table origin-column target-type target-table target-column]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s) DEFERRABLE INITIALLY DEFERRED;\n"
            origin-table origin-column target-type target-table target-column)))
