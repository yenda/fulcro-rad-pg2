(ns tasks.lint
  (:require
   [babashka.process :refer [shell]]
   [clojure.string :as str]
   [tasks.utils.common :as common]))

(def ^:private clj-kondo-install-hint
  "Please install clj-kondo. See https://github.com/clj-kondo/clj-kondo#installation")

(defn run-clj-kondo
  "Run clj-kondo on src directory with two passes:
   1. First strict check for errors only
   2. Second pass for unused requires/imports if no errors found"
  []
  (if (common/require-tool! "clj-kondo" clj-kondo-install-hint)
    (do
      (println "Linting src directory with clj-kondo for errors...")

      (let [;; First pass: Check for errors only (ignoring unused vars/imports)
            first-result (shell {:continue true
                                 :out :string
                                 :err :string}
                                (str "clj-kondo "
                                     "--config-dir .clj-kondo "
                                     "--parallel "
                                     "--config '{:linters {"
                                     ":unused-namespace {:level :off} "
                                     ":unused-referred-var {:level :off} "
                                     ":unused-import {:level :off}}"
                                     "}' "
                                     "--report-level error --lint src"))
            first-output (:out first-result)
            error-match (re-find #"errors: (\d+)" first-output)
            error-count (if error-match
                          (Integer/parseInt (second error-match))
                          0)]

        (when-not (str/blank? first-output)
          (println first-output))
        (when-not (str/blank? (:err first-result))
          (println (:err first-result)))

        (if (zero? error-count)
          ;; No errors found, now do second pass for unused requires/imports
          (do
            (println "Checking for unused requires/imports...")
            (let [second-result (shell {:continue true
                                        :out :string
                                        :err :string}
                                       "clj-kondo --config-dir .clj-kondo --parallel --lint src")
                  second-output (:out second-result)]

              (when-not (str/blank? second-output)
                (println second-output))
              (when-not (str/blank? (:err second-result))
                (println (:err second-result)))

              (let [warning-match (re-find #"warnings: (\d+)" second-output)
                    warning-count (if warning-match
                                    (Integer/parseInt (second warning-match))
                                    0)
                    has-warnings? (> warning-count 0)]

                (cond
                  has-warnings?
                  {:success? false :has-warnings? true :warning-count warning-count :error-count 0}

                  :else
                  {:success? true :has-warnings? false :warning-count 0 :error-count 0}))))
          ;; Errors found in first pass
          {:success? false :has-warnings? false :warning-count 0 :error-count error-count})))
    {:success? false :has-warnings? false :warning-count 0 :error-count 0}))

(defn lint
  "Lint src directory. Fails on errors or warnings."
  [& _]
  (println "Running clj-kondo linter on src directory")

  (let [lint-result (run-clj-kondo)
        error-count (get lint-result :error-count 0)
        warning-count (get lint-result :warning-count 0)]

    (cond
      (pos? error-count)
      (do
        (println (format "Linting found %d critical error(s)!" error-count))
        (System/exit 1))

      (:has-warnings? lint-result)
      (do
        (println (format "Linting found %d warning(s). Please fix them." warning-count))
        (System/exit 1))

      (:success? lint-result)
      (println "All files pass linting without errors or warnings!")

      :else
      (do
        (println "Linting failed!")
        (System/exit 1)))))

(defn install-configs
  "Install clj-kondo configs for dependencies in the classpath."
  [& _]
  (println "Installing clj-kondo configurations for dependencies...")
  (if (common/require-tool! "clj-kondo" clj-kondo-install-hint)
    (try
      (println "Getting classpath...")
      (let [cp-result (shell {:out :string :err :string :continue true}
                             "clojure" "-A:test" "-Spath")
            classpath (str/trim (:out cp-result))]
        (if (or (not (zero? (:exit cp-result))) (str/blank? classpath))
          (do
            (println "Failed to get classpath:")
            (println (:err cp-result))
            false)
          (do
            (println "Installing dependency configurations from classpath...")
            (let [result (shell {:continue true
                                 :out :string
                                 :err :string}
                                "clj-kondo" "--copy-configs" "--dependencies" "--skip-lint" "--lint" classpath)]
              (if (zero? (:exit result))
                (do
                  (when-not (str/blank? (:out result))
                    (println (:out result)))
                  (println "Dependency configurations successfully installed!")
                  true)
                (do
                  (println "Error installing configurations:")
                  (println (:err result))
                  false))))))
      (catch Exception e
        (println "Error installing configurations:" (ex-message e))
        false))
    false))
