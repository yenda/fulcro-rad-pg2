(ns com.fulcrologic.rad.database-adapters.sql.plugin
  "RAD SQL plugin for pg2 PostgreSQL driver."
  (:require
   [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
   [com.fulcrologic.rad.database-adapters.sql :as sql]
   [com.fulcrologic.rad.database-adapters.sql.write :as sql.write]
   [com.fulcrologic.rad.form :as form]
   [taoensso.timbre :as log]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pathom Environment

(defn wrap-env
  "Env middleware to add the necessary SQL connections and databases to the pathom env for
   a given request. Requires a database-mapper, which is a
   `(fn [pathom-env] {schema-name connection-pool})` for a given request.

  The resulting pathom-env available to all resolvers will then have:

  - `::sql/connection-pools`: The result of the database-mapper.
  "
  ([_all-attributes database-mapper config] (wrap-env _all-attributes nil database-mapper config))
  ([_all-attributes base-wrapper database-mapper config]
   (let [database-configs (get config ::sql/databases)]
     (doseq [[k v] database-configs]
       (let [{:sql/keys [schema]} v]
         (log/info k "using pg2 PostgreSQL driver for schema" schema)))
     (fn [env]
       (cond-> (let [database-connection-map (database-mapper env)]
                 (assoc env ::sql/connection-pools database-connection-map))
         base-wrapper (base-wrapper))))))

(defn pathom-plugin
  "A pathom 2 plugin that adds the necessary SQL connections and databases to the pathom env for
   a given request. Requires a database-mapper, which is a
  `(fn [pathom-env] {schema-name connection-pool})` for a given request.

  See also wrap-env.

  The resulting pathom-env available to all resolvers will then have:

  - `::sql/connection-pools`: The result of the database-mapper.

  This plugin should run before (be listed after) most other plugins in the plugin chain since
  it adds connection details to the parsing env.
  "
  ([database-mapper]
   (pathom-plugin database-mapper {}))
  ([database-mapper config]
   (let [augment (wrap-env nil nil database-mapper config)]
     {:com.wsscode.pathom.core/wrap-parser
      (fn env-wrap-wrap-parser [parser]
        (fn env-wrap-wrap-internal [env tx]
          (parser (augment env) tx)))})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Form Middleware

(defn wrap-sql-save
  "Form save middleware to accomplish SQL saves."
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [save-result (sql.write/save-form! pathom-env params)]
       save-result)))
  ([handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [save-result (sql.write/save-form! pathom-env params)
           handler-result (handler pathom-env)]
       (deep-merge save-result handler-result)))))
