(ns com.fulcrologic.rad.database-adapters.sql.perf.benchmark
  "Benchmark utilities for performance testing.

   Provides statistical measurement of operations with:
   - Configurable iterations and warm-up
   - Min/max/mean/median/p95/p99 statistics
   - Formatted output for comparison"
  (:require
   [clojure.string :as str]))

(defn percentile
  "Calculate the nth percentile of a sorted sequence of numbers."
  [sorted-values n]
  (let [cnt (count sorted-values)
        idx (-> (* n cnt)
                (/ 100)
                (Math/ceil)
                (dec)
                (max 0)
                (min (dec cnt))
                int)]
    (nth sorted-values idx)))

(defn statistics
  "Calculate statistics for a sequence of timing values (in ms)."
  [times]
  (let [sorted (vec (sort times))
        cnt (count sorted)]
    {:min (first sorted)
     :max (last sorted)
     :mean (/ (reduce + times) cnt)
     :median (percentile sorted 50)
     :p95 (percentile sorted 95)
     :p99 (percentile sorted 99)
     :count cnt}))

(defn format-stats
  "Format statistics for display."
  [{:keys [min max mean median p95 p99 count]}]
  (format "n=%d  min=%.2f  mean=%.2f  median=%.2f  p95=%.2f  p99=%.2f  max=%.2f ms"
          count min mean median p95 p99 max))

(defn measure-ns
  "Measure execution time in nanoseconds."
  [f]
  (let [start (System/nanoTime)
        result (f)
        elapsed (- (System/nanoTime) start)]
    {:result result :elapsed-ns elapsed}))

(defn run-benchmark
  "Run a benchmark with warm-up iterations and return statistics.

   Options:
   - :warmup - number of warm-up iterations (default 5)
   - :iterations - number of measured iterations (default 50)
   - :setup - function called before each iteration (optional)
   - :teardown - function called after each iteration (optional)

   Returns map with :stats and :results (last result)"
  ([f] (run-benchmark f {}))
  ([f {:keys [warmup iterations setup teardown]
       :or {warmup 5 iterations 50}}]
   ;; Warm-up phase
   (dotimes [_ warmup]
     (when setup (setup))
     (f)
     (when teardown (teardown)))

   ;; Measurement phase
   (let [times (atom [])]
     (dotimes [_ iterations]
       (when setup (setup))
       (let [{:keys [result elapsed-ns]} (measure-ns f)]
         (swap! times conj (/ elapsed-ns 1e6)) ; convert to ms
         (when teardown (teardown))))
     {:stats (statistics @times)
      :times @times})))

(defn benchmark!
  "Run a named benchmark and print results.

   Options same as run-benchmark plus:
   - :description - optional longer description"
  [name f & {:keys [description] :as opts}]
  (print (str "  " name "... "))
  (flush)
  (let [{:keys [stats]} (run-benchmark f opts)]
    (println (format-stats stats))
    stats))

(defmacro with-benchmark
  "Convenience macro for running a benchmark.

   (with-benchmark \"my-test\" {:iterations 100}
     (do-something))"
  [name opts & body]
  `(benchmark! ~name (fn [] ~@body) ~@(mapcat identity opts)))

(defn print-header
  "Print a section header for benchmark output."
  [title]
  (println)
  (println (str "=== " title " ==="))
  (println))

(defn compare-stats
  "Compare two stats maps and return percentage difference in mean."
  [baseline current]
  (let [baseline-mean (:mean baseline)
        current-mean (:mean current)
        diff-pct (* 100 (/ (- current-mean baseline-mean) baseline-mean))]
    {:baseline-mean baseline-mean
     :current-mean current-mean
     :diff-pct diff-pct
     :faster? (neg? diff-pct)}))
