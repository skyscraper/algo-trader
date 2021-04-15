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

(defn initialize-positions []
  (reset!
   positions
   (vec
    (repeatedly
     fc-count
     (fn [] (atom {:cash 10000.0 :shares 0.0 :port-val 10000.0}))))))

(defn update-port! [price target side p]
  (let [target (if (> target @oms/max-pos-notional)
                 @oms/max-pos-notional
                 (if (< target (- @oms/max-pos-notional))
                   (- @oms/max-pos-notional)
                   target))
        {:keys [port-val]}
        (swap! p
               (fn [{:keys [cash shares]}]
                 (let [pos-mtm (* shares price)
                       delta-cash (- target pos-mtm)
                       new-cash (- cash delta-cash)
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
                       port-val (+ cash (* shares slip-price))]
                   {:cash new-cash :shares new-shares :port-val port-val})))]
    port-val))

(def test-time 1618445135)

(defn run
  "run a backtest for a single market, outputting portfolio prices for each window"
  [market]
  (log/info (format "starting backtest for %s" (name market)))
  (reset! core/markets [market])
  (model/initialize (select-keys (:target-amts config) [market]))
  (oms/initialize-equity 100000.0)
  (let [max-pos (oms/determine-notionals @core/markets)]
    (log/info (format "max notional per market: %f" max-pos))
    (log/info (format "expected abs position size, pre-vol scale: %f"
                      (model/set-scale-target max-pos))))
  (initialize-positions)
  (log/info "fetching trades...")
  (let [bar-count (atom 0)
        trades (api/historical-trades market test-time 1)
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

