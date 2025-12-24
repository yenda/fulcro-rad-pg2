(ns com.fulcrologic.rad.database-adapters.sql.vendor
  "PostgreSQL vendor adapter for constraint handling.")

(defprotocol VendorAdapter
  (relax-constraints! [this connection execute-fn]
    "Defer constraint checking until end of txn using provided execute-fn.")
  (add-referential-column-statement [this origin-table origin-column target-type target-table target-column]
    "Alter table and add a FK column."))

(deftype PostgreSQLAdapter []
  VendorAdapter
  (relax-constraints! [_ conn execute-fn]
    (execute-fn conn ["SET CONSTRAINTS ALL DEFERRED"]))
  (add-referential-column-statement [_ origin-table origin-column target-type target-table target-column]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s) DEFERRABLE INITIALLY DEFERRED;\n"
            origin-table origin-column target-type target-table target-column)))
