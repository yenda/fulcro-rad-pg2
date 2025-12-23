(ns tasks.utils.common
  "Shared utilities for babashka tasks."
  (:require
   [babashka.process :as p]
   [clojure.string :as str]))

;; =============================================================================
;; Terminal Colors
;; =============================================================================

(def colors
  {:green "\u001B[32m"
   :yellow "\u001B[33m"
   :red "\u001B[31m"
   :reset "\u001B[0m"})

(defn colorize [color text]
  (str (get colors color "") text (:reset colors)))

;; =============================================================================
;; Shell Command Execution
;; =============================================================================

(defn shell-result
  "Execute a shell command and return a result map."
  [& args]
  (let [[opts cmd-args] (if (map? (first args))
                          [(first args) (rest args)]
                          [{} args])
        proc-opts (merge {:out :string :err :string :continue true}
                         (select-keys opts [:dir]))]
    (try
      (let [result (apply p/shell proc-opts cmd-args)]
        {:success? (zero? (:exit result))
         :exit (:exit result)
         :out (or (:out result) "")
         :err (or (:err result) "")})
      (catch Exception e
        {:success? false
         :exit 1
         :out ""
         :err (ex-message e)}))))

(defn shell-ok? [& args]
  (:success? (apply shell-result args)))

;; =============================================================================
;; Tool Detection
;; =============================================================================

(defn tool-installed? [tool-name]
  (shell-ok? "which" tool-name))

(defn require-tool!
  "Check if a tool is installed, print error and return false if not."
  ([tool-name]
   (require-tool! tool-name nil))
  ([tool-name install-hint]
   (if (tool-installed? tool-name)
     true
     (do
       (println (colorize :yellow (str "\n" tool-name " not found in PATH!")))
       (when install-hint
         (println install-hint))
       false))))
