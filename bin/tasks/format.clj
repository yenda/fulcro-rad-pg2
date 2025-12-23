(ns tasks.format
  (:require
   [babashka.process :refer [shell]]
   [tasks.utils.common :as common]
   [tasks.utils.prompt :as prompt]))

(def ^:private cljfmt-install-hint
  (str "Please install cljfmt by running:\n"
       "/bin/bash -c \"$(curl -fsSL https://raw.githubusercontent.com/weavejester/cljfmt/HEAD/install.sh)\"\n"
       "Or see https://github.com/weavejester/cljfmt for installation instructions."))

(defn run-cljfmt
  "Run cljfmt on source directories with project configuration from .cljfmt.edn"
  [& {:keys [check-only]}]
  (if (common/require-tool! "cljfmt" cljfmt-install-hint)
    (do
      (println (if check-only "Checking" "Formatting") "with cljfmt...")
      (let [cmd (str "cljfmt --config .cljfmt.edn " (if check-only "check" "fix"))
            result (shell {:continue true
                           :err :string}
                          cmd)]
        (when-not (zero? (:exit result))
          (println (:err result)))
        (zero? (:exit result))))
    (System/exit 1)))

(defn format-task
  "Format task that checks files for formatting issues and exits with appropriate code."
  []
  (if (run-cljfmt :check-only true)
    (println "All files are formatted correctly!")
    (do
      (println "Formatting issues found!")
      (if (prompt/yes-no "Would you like to fix these formatting issues?")
        (do
          (println "Fixing formatting issues...")
          (if (run-cljfmt)
            (println "All files formatted successfully!")
            (do
              (println "Some errors occurred during formatting.")
              (System/exit 1))))
        (do
          (println "Formatting issues found but not fixed.")
          (System/exit 1))))))
