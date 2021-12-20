-(ns algo-trader.backtest
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config fc-count]]
            [algo-trader.db :as db]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [get-target-sizes]]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [java-time :refer [as duration]]
            [taoensso.timbre :as log]))

(def header-base [:raw-fc :scale :scaled-fc])
(def rest-header [:sigma :combined :fdm-fc :port-val])
(def header (let [xs (mapcat
                      (fn [i]
                        (map #(str (name %) i) header-base))
                      (range fc-count))]
              (vec (concat xs (mapv name rest-header)))))

(defn run
  "run a backtest for a single market, outputting portfolio prices for each window"
  [market verbose?]
  (let [target-size (market (get-target-sizes))
        m-list [market]]
    (statsd/reset-statsd!)
    (log/info (format "starting backtest for %s" (name market)))
    (model/initialize {market target-size})
    (oms/initialize-equity (:test-trading-capital config))
    (let [starting-cash (oms/determine-starting-capital-per-market [m-list])]
      (log/info (format "starting equity per market: %,.2f" starting-cash))
      (oms/initialize-positions m-list starting-cash))
    (log/info "fetching trades...")
    (let [end (db/get-last-ts market)
          begin (- end (as (duration (:total-days config) :days) :millis))
          backtest-trades (db/get-trades-memo market begin end)
          backtest-bars (reverse (bars/generate-bars target-size backtest-trades))]
      (log/info (format "processing %s trades..." (count backtest-trades)))
      (with-open [writer (io/writer (format "resources/%s_backtest.csv" (name market)))]
        (write-csv writer (if verbose? [header] [["port-val"]]))
        (let [record-fn (fn [x] (write-csv writer [x]))]
          (model/evaluate-model market backtest-bars record-fn verbose?)))
      (log/info (format "tested on %s bars" (count backtest-bars))))))

(defn portfolio-opt-inputs [market]
  (let [m-list [market]
        target-size (market (get-target-sizes))]
    (statsd/reset-statsd!)
    (log/info (format "starting backtest for %s" (name market)))
    (log/info "fetching trades...")
    (let [end (db/get-last-ts market)
          begin (- end (as (duration (:total-days config) :days) :millis))
          backtest-trades (db/get-trades market begin end)
          backtest-bars (reverse (bars/generate-bars target-size backtest-trades))]
      (log/info (format "processing %s trades..." (count backtest-trades)))
      (let [all (atom [])
            ws (vec (repeat fc-count 0.0))]
        (doseq [i (range fc-count)
                :let [a (atom [])]]
          (alter-var-root #'model/weights (fn [_] (assoc ws i 1.0)))
          (model/initialize {market target-size})
          (oms/initialize-equity (:test-trading-capital config))
          (let [starting-cash (oms/determine-starting-capital-per-market [m-list])]
            (log/info (format "starting equity per market: %,.2f" starting-cash))
            (oms/initialize-positions m-list starting-cash))
          (let [record-fn (fn [[x]] (swap! a conj x))]
            (model/evaluate-model market backtest-bars record-fn false))
          (swap! all conj @a))
        (let [transpose (apply mapv vector @all)]
          (with-open [writer (io/writer (format "resources/%s_port_prices.csv" (name market)))]
            (write-csv writer [(for [f [:ewm-oi]
                                     i (range fc-count)]
                                 (str (name f) "-" i))])
            (write-csv writer transpose))))
      (log/info (format "tested on %s bars" (count backtest-bars))))))

(defn run-all []
  (let [markets (keys (get-target-sizes))]
    (doseq [market markets]
      (run market false))))
