(ns tasks.check
  (:require
   [tasks.format :as format]
   [tasks.lint :as lint]))

(defn check
  "Run all checks (format, lint).
   Exits with code 1 if any check fails."
  [& _]
  (println "\n=== Running Format Check ===")
  (format/format-task)

  (println "\n=== Running Lint Check ===")
  (lint/lint)

  (println "\n=== Check Summary ===")
  (println "All checks completed successfully!"))
