(ns algo-trader.statsd
  (:import [com.timgroup.statsd NonBlockingStatsDClient])
  (:refer-clojure :exclude [count]))

(def statsd (atom nil))

(defn reset-statsd! []
  (reset! statsd (new NonBlockingStatsDClient "statsd" "localhost" 8125)))

(defn str-arr [xs]
  (into-array String (map name xs)))

(defn count [metric delta tags]
  (.count @statsd (name metric) (long delta) (str-arr tags)))

(defn gauge [metric value tags]
  (.recordGaugeValue @statsd (name metric) (double value) (str-arr tags)))

(defn distribution [metric value tags]
  (.recordDistributionValue @statsd (name metric) (double value) (str-arr tags)))
