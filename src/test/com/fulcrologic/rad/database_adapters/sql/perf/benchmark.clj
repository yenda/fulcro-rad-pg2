(ns com.fulcrologic.rad.database-adapters.sql.perf.benchmark
  "Benchmark utilities using criterium for proper JVM benchmarking.

   Criterium handles:
   - JIT warmup estimation
   - GC between measurements
   - Statistical analysis with confidence intervals
   - Outlier detection"
  (:require
   [criterium.core :as crit]))

;; =============================================================================
;; Criterium-based benchmarking
;; =============================================================================

(defn- safe-first-ms
  "Safely extract first value and convert to ms, returning 0 if nil."
  [coll]
  (if-let [v (first coll)]
    (* 1e3 v)
    0.0))

(defn quick-bench*
  "Run a quick benchmark and return results map.
   Uses criterium's quick-bench which is suitable for operations > 1ms."
  [f]
  (let [results (crit/quick-benchmark* f {})]
    {:mean (safe-first-ms (:mean results))
     :std-dev (safe-first-ms (:std-dev results))
     :variance (if-let [v (first (:variance results))] (* 1e6 v) 0.0)
     :lower-q (safe-first-ms (:lower-q results))
     :upper-q (safe-first-ms (:upper-q results))
     :samples (or (:sample-count results) 0)
     :outliers (:outliers results)
     :overhead-ns (:overhead results)}))

(defn bench*
  "Run a thorough benchmark and return results map.
   Takes longer but provides more accurate results."
  [f]
  (let [results (crit/benchmark* f {})]
    {:mean (safe-first-ms (:mean results))
     :std-dev (safe-first-ms (:std-dev results))
     :variance (if-let [v (first (:variance results))] (* 1e6 v) 0.0)
     :lower-q (safe-first-ms (:lower-q results))
     :upper-q (safe-first-ms (:upper-q results))
     :samples (or (:sample-count results) 0)
     :outliers (:outliers results)
     :overhead-ns (:overhead results)}))

(defn format-criterium-stats
  "Format criterium statistics for display."
  [{:keys [mean std-dev lower-q upper-q samples]}]
  (format "mean=%.3f ms ±%.3f  [%.3f, %.3f]  n=%d"
          mean std-dev lower-q upper-q samples))

(defn benchmark!
  "Run a named benchmark using criterium and print results.

   Options:
   - :thorough? - use full benchmark instead of quick-bench (default false)
   - :iterations, :warmup - ignored (criterium handles this automatically)"
  ([name f] (benchmark! name f {}))
  ([name f opts]
   (print (str "  " name "... "))
   (flush)
   (let [bench-fn (if (:thorough? opts) bench* quick-bench*)
         stats (bench-fn f)]
     (println (format-criterium-stats stats))
     stats)))

(defmacro with-benchmark
  "Convenience macro for running a benchmark.

   (with-benchmark \"my-test\" {}
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

(defn format-comparison
  "Format a comparison between baseline and current stats."
  [name baseline current]
  (let [{:keys [baseline-mean current-mean diff-pct faster?]} (compare-stats baseline current)]
    (format "%-45s baseline: %.3f ms  current: %.3f ms  change: %+.1f%% %s"
            name baseline-mean current-mean diff-pct
            (if faster? "✓" ""))))

;; =============================================================================
;; Legacy utilities (kept for backward compatibility)
;; =============================================================================

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
  "Calculate statistics for a sequence of timing values (in ms).
   DEPRECATED: Use criterium-based functions instead."
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
