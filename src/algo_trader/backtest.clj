(ns algo-trader.backtest
  (:require [algo-trader.api :as api]
            [algo-trader.bars :as bars]
            [algo-trader.config :refer [config hardcoded-eq]]
            [algo-trader.core :as core]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.utils :refer [ewm-vol uc-kw]]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]))

(defn run
  "run a backtest for a single market, outputting portfolio prices for each window"
  [underlying test-time]
  (let [market (uc-kw (str (name underlying) "-PERP"))
        m-list [market]]
    (log/info (format "starting backtest for %s" (name underlying)))
    (model/set-scale-target!)
    (let [target-amts (select-keys (api/get-futures-targets underlying) m-list)]
      (model/initialize target-amts)
      (reset! core/markets (keys target-amts)))
    (oms/initialize-equity hardcoded-eq)
    (let [starting-cash (oms/determine-notionals @core/markets)]
      (log/info (format "starting equity per market: %,.2f" starting-cash))
      (oms/initialize-positions m-list starting-cash))
    (log/info "fetching trades...")
    (let [bar-count (atom 0)
          trades (api/historical-trades market (or test-time (long (System/currentTimeMillis))) 1)
          data (market model/model-data)]
      (log/info (format "processing %s trades..." (count trades)))
      (with-open [writer (io/writer "resources/backtest.csv")]
        (doseq [trade trades]
          (let [{:keys [price side] :as trade} (update trade :side keyword)]
            (swap! data bars/add-to-bars trade)
            (when (> (count (:bars @data)) (:bar-count config))
              (swap! bar-count inc)
              (let [{:keys [ewmacs bars]} (swap! data update :bars #(take (:bar-count config) %))
                    vol (ewm-vol (map :diff bars))
                    forecasts (map-indexed
                               (fn [idx ewmac] (model/predict-single market vol idx ewmac))
                               ewmacs)
                    window-vals (map-indexed
                                 (fn [idx forecast]
                                   (oms/update-port! price forecast side (nth oms/positions idx)))
                                 forecasts)]
                (write-csv writer [(concat forecasts window-vals)]))))))
      (log/info (format "created %s bars" (+ (:bar-count config) @bar-count))))))

