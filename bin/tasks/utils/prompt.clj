(ns tasks.utils.prompt
  (:require
   [clojure.string :as str]))

(defn is-ci?
  "Returns true if running in CI environment."
  []
  (= (System/getenv "CI") "true"))

(defn yes-no
  "Prompt user for yes/no response.
   Defaults to 'y' if user just presses Enter.
   Returns false in CI mode."
  [prompt]
  (if (is-ci?)
    (do
      (println "CI mode detected, defaulting to 'n'")
      false)
    (do
      (print (str prompt " (Y/n) "))
      (flush)
      (let [response (str/lower-case (str/trim (or (read-line) "y")))]
        (cond
          (or (str/blank? response) (contains? #{"y" "yes"} response)) true
          (contains? #{"n" "no"} response) false
          :else (do
                  (println "Please answer 'y' or 'n'")
                  (recur prompt)))))))
