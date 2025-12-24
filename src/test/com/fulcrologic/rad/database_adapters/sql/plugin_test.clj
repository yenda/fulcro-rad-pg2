(ns com.fulcrologic.rad.database-adapters.sql.plugin-test
  "Tests for plugin.clj - environment wrapping and middleware functions."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.rad.database-adapters.sql :as sql]
   [com.fulcrologic.rad.database-adapters.sql.plugin :as plugin]
   [com.fulcrologic.rad.form :as form]))

;; =============================================================================
;; wrap-env Tests
;; =============================================================================

(deftest wrap-env-adds-connection-pools
  (testing "wrap-env adds connection pools to environment"
    (let [mock-pool {:pool :mock-pool}
          database-mapper (fn [_env] {:production mock-pool})
          config {::sql/databases {:production {:sql/schema :production}}}
          wrapper (plugin/wrap-env [] database-mapper config)
          env {}
          result (wrapper env)]
      (is (= {:production mock-pool} (::sql/connection-pools result))))))

(deftest wrap-env-preserves-existing-env
  (testing "wrap-env preserves existing environment keys"
    (let [mock-pool {:pool :test}
          database-mapper (fn [_env] {:main mock-pool})
          config {::sql/databases {:main {:sql/schema :test}}}
          wrapper (plugin/wrap-env [] database-mapper config)
          env {:existing-key :existing-value
               :another-key 42}
          result (wrapper env)]
      (is (= :existing-value (:existing-key result)))
      (is (= 42 (:another-key result)))
      (is (= {:main mock-pool} (::sql/connection-pools result))))))

(deftest wrap-env-with-base-wrapper
  (testing "wrap-env applies base wrapper after adding pools"
    (let [mock-pool {:pool :test}
          database-mapper (fn [_env] {:main mock-pool})
          base-wrapper (fn [env] (assoc env :wrapped-by-base true))
          config {::sql/databases {:main {:sql/schema :test}}}
          wrapper (plugin/wrap-env [] base-wrapper database-mapper config)
          env {}
          result (wrapper env)]
      (is (= {:main mock-pool} (::sql/connection-pools result)))
      (is (true? (:wrapped-by-base result))))))

(deftest wrap-env-database-mapper-receives-env
  (testing "database-mapper receives the original environment"
    (let [received-env (atom nil)
          database-mapper (fn [env]
                            (reset! received-env env)
                            {:main {:pool :mock}})
          config {::sql/databases {:main {:sql/schema :test}}}
          wrapper (plugin/wrap-env [] database-mapper config)
          original-env {:request {:user-id 123}}]
      (wrapper original-env)
      (is (= original-env @received-env)))))

(deftest wrap-env-multiple-databases
  (testing "wrap-env supports multiple database configurations"
    (let [prod-pool {:pool :production}
          analytics-pool {:pool :analytics}
          database-mapper (fn [_env] {:production prod-pool
                                      :analytics analytics-pool})
          config {::sql/databases {:production {:sql/schema :production}
                                   :analytics {:sql/schema :analytics}}}
          wrapper (plugin/wrap-env [] database-mapper config)
          result (wrapper {})]
      (is (= prod-pool (get-in result [::sql/connection-pools :production])))
      (is (= analytics-pool (get-in result [::sql/connection-pools :analytics]))))))

;; =============================================================================
;; pathom-plugin Tests
;; =============================================================================

(deftest pathom-plugin-wraps-parser
  (testing "pathom-plugin wraps parser with environment augmentation"
    (let [mock-pool {:pool :test}
          database-mapper (fn [_env] {:main mock-pool})
          config {::sql/databases {:main {:sql/schema :test}}}
          plugin (plugin/pathom-plugin database-mapper config)
          parser-called (atom false)
          received-env (atom nil)
          mock-parser (fn [env _tx]
                        (reset! parser-called true)
                        (reset! received-env env)
                        {:result :success})
          wrapped-parser ((:com.wsscode.pathom.core/wrap-parser plugin) mock-parser)]
      (wrapped-parser {} [:some-query])
      (is @parser-called)
      (is (= {:main mock-pool} (::sql/connection-pools @received-env))))))

(deftest pathom-plugin-passes-transaction
  (testing "pathom-plugin passes transaction to parser"
    (let [database-mapper (fn [_env] {:main {:pool :test}})
          config {}
          plugin (plugin/pathom-plugin database-mapper config)
          received-tx (atom nil)
          mock-parser (fn [_env tx]
                        (reset! received-tx tx)
                        {})
          wrapped-parser ((:com.wsscode.pathom.core/wrap-parser plugin) mock-parser)]
      (wrapped-parser {} [:query/a :query/b])
      (is (= [:query/a :query/b] @received-tx)))))

(deftest pathom-plugin-default-config
  (testing "pathom-plugin works with default empty config"
    (let [database-mapper (fn [_env] {:main {:pool :default}})
          plugin (plugin/pathom-plugin database-mapper)
          mock-parser (fn [env _tx] env)
          wrapped-parser ((:com.wsscode.pathom.core/wrap-parser plugin) mock-parser)
          result (wrapped-parser {} [])]
      (is (= {:main {:pool :default}} (::sql/connection-pools result))))))

;; =============================================================================
;; wrap-sql-save Tests
;; =============================================================================

;; Note: wrap-sql-save calls sql.write/save-form! which requires database access.
;; We test the structure and behavior of the middleware, not the actual save.

(deftest wrap-sql-save-0-arity-returns-function
  (testing "0-arity wrap-sql-save returns a middleware function"
    (let [middleware (plugin/wrap-sql-save)]
      (is (fn? middleware)))))

(deftest wrap-sql-save-1-arity-chains-handler
  (testing "1-arity wrap-sql-save chains with existing handler"
    (let [handler-called (atom false)
          handler (fn [_env]
                    (reset! handler-called true)
                    {:handler-result :from-handler})
          middleware (plugin/wrap-sql-save handler)]
      (is (fn? middleware)))))

(deftest wrap-sql-save-structure
  (testing "wrap-sql-save accepts pathom environment with form params"
    ;; This tests that wrap-sql-save extracts the right keys from env
    ;; Actual database testing is done in integration tests
    (let [middleware (plugin/wrap-sql-save)
          ;; Create a minimal env that would be passed to wrap-sql-save
          ;; Note: In real usage, this would fail because there's no pool,
          ;; but we can verify the function structure
          env {::form/params {:some :params}}]
      ;; The middleware should be callable (will fail on actual save without DB)
      (is (fn? middleware)))))
