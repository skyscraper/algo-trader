(ns algo-trader.backtest
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config total-fc-count]]
            [algo-trader.db :as db]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.utils :refer [get-target-amts]]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [java-time :refer [as duration]]))

(def header-base [:raw-fc :scale :scaled-fc])
(def rest-header [:combined :fdm-fc :port-val])
(def header (let [xs (mapcat
                      (fn [i]
                        (map #(str (name %) i) header-base))
                      (range total-fc-count))]
              (vec (concat xs (mapv name rest-header)))))

(defn run
  "run a backtest for a single market, outputting portfolio prices for each window"
  [market target-amt verbose?]
  (let [m-list [market]]
    (log/info (format "starting backtest for %s" (name market)))
    (model/set-scale-target!)
    (model/initialize {market target-amt})
    (oms/initialize-equity (:test-market-notional config))
    (let [starting-cash (oms/determine-notionals [m-list])]
      (log/info (format "starting equity per market: %,.2f" starting-cash))
      (oms/initialize-positions m-list starting-cash))
    (log/info "fetching trades...")
    (let [end (db/get-last-ts market)
          begin (- end (as (duration (:total-days config) :days) :millis))
          backtest-trades (db/get-trades market begin end)
          backtest-bars (reverse (bars/generate-bars target-amt backtest-trades))]
      (log/info (format "processing %s trades..." (count backtest-trades)))
      (with-open [writer (io/writer (format "resources/%s_backtest.csv" (name market)))]
        (write-csv writer (if verbose? [header] [["port-val"]]))
        (let [record-fn (fn [x] (write-csv writer [x]))]
          (model/evaluate-model market backtest-bars record-fn verbose?)))
      (log/info (format "tested on %s bars" (count backtest-bars))))))

(defn run-all []
  (let [target-amts (get-target-amts)]
    (doseq [[market target-amt] target-amts]
      (run market target-amt false))))
