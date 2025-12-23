(ns com.fulcrologic.rad.database-adapters.sql.result-set
  "This namespace provides some tools to work with jdbc result sets"
  (:require [next.jdbc.result-set :as jdbc.rs])
  (:import (java.sql Array)))

(defn coerce-result-sets!
  "Will extend the JDBC ReadableColumn protocol to coerce common
  values like vectors, dates and json objects to clojure
  datastructures. This function must be called in order for RAD to
  function properly."
  []
  (extend-protocol jdbc.rs/ReadableColumn
    Array
    (read-column-by-label [v _] (vec (.getArray v)))
    (read-column-by-index [v _ _] (vec (.getArray v)))))
