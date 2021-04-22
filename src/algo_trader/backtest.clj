(ns algo-trader.backtest
  (:require [algo-trader.api :as api]
            [algo-trader.bars :as bars]
            [algo-trader.config :refer [config fc-count]]
            [algo-trader.core :as core]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.utils :refer [ewm-vol]]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]))

(def positions (atom {}))

(defn initialize-positions [starting-cash]
  (reset!
   positions
   (vec
    (repeatedly
     fc-count
     (fn [] (atom {:cash starting-cash :shares 0.0 :port-val starting-cash}))))))

(defn update-port! [price target side p]
  (let [target-notional (oms/get-target target)
        {:keys [port-val]}
        (swap!
         p
         (fn [{:keys [cash shares]}]
           (let [pos-mtm (* shares price)
                 delta-cash (- target-notional pos-mtm)]
             (if (>= (Math/abs delta-cash) @oms/min-order-notional)
               (let [new-cash (- cash delta-cash)
                     ;; if we are going same way, only 1bps slippage, otherwise 5bps to cross spread
                     slip-price (if (pos? delta-cash)
                                  (if (= :buy side)
                                    (* price 1.0001)
                                    (* price 1.0005))
                                  (if (= :buy side)
                                    (* price 0.9995)
                                    (* price 0.9999)))
                     ;; ftx taker fees are 7bps for lowest tier
                     my-price (if (pos? delta-cash)
                                (* slip-price 1.0007)
                                (* slip-price 0.9993))
                     delta-shares (/ delta-cash my-price)
                     new-shares (+ shares delta-shares)
                     port-val (+ new-cash (* new-shares slip-price))]
                 {:cash new-cash :shares new-shares :port-val port-val})
               {:cash cash :shares shares :port-val (+ cash (* shares price))}))))]
    port-val))

(defn run
  "run a backtest for a single market, outputting portfolio prices for each window"
  [market test-time]
  (log/info (format "starting backtest for %s" (name market)))
  (model/set-scale-target!)
  (let [target-amts (select-keys (api/get-futures-targets) [market])]
    (model/initialize target-amts)
    (reset! core/markets (keys target-amts)))
  (oms/initialize-equity (* (:num-markets config) (:max-pos-notional config)))
  (let [max-pos (oms/determine-notionals @core/markets)]
    (log/info (format "max notional per market: %f" max-pos))
    (initialize-positions max-pos))
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
                                 (update-port! price forecast side (nth @positions idx)))
                               forecasts)]
              (write-csv writer [(concat forecasts window-vals)]))))))
    (log/info (format "created %s bars" (+ (:bar-count config) @bar-count)))))

