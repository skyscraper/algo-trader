(ns algo-trader.backtest
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config]]
            [algo-trader.db :as db]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.utils :refer [clip get-target-amts]]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]))

(defn fc-scale-val [market features vol price side]
  (let [pred (first (model/model-predict market features))
        raw-fc (/ pred vol)
        scale (model/update-and-get-forecast-scale! market raw-fc)
        scaled-fc (clip model/fc-cap (* raw-fc scale))
        fdm-fc (clip model/fc-cap (* scaled-fc model/fdm))
        port-val (oms/update-port! price fdm-fc side vol (market oms/positions))]
    [pred raw-fc scaled-fc fdm-fc scale port-val]))

(def header ["pred" "raw-fc" "scaled-fc" "fdm-fc" "scale" "port-val"])
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
    (let [bar-count (atom 0)
          begin (db/get-first-ts market)
          end (db/get-last-ts market)
          split-ts (long (+ begin (* (:training-split config) (- end begin))))
          training-data (db/get-trades market begin split-ts)
          backtest-trades (db/get-trades market split-ts end)
          data (market model/model-data)]
      (log/info "generating models...")
      (model/generate-model market target-amt training-data)
      (log/info (format "processing %s trades..." (count backtest-trades)))
      (with-open [writer (io/writer (format "resources/%s_backtest.csv" (name market)))]
        (write-csv writer (if verbose? [header] [["port-val"]]))
        (doseq [trade backtest-trades]
          (let [{:keys [price side] :as trade} (update trade :side keyword)]
            (swap! data bars/add-to-bars trade)
            (when (> (count (:bars @data)) (:bar-count config))
              (swap! bar-count inc)
              (let [{:keys [bars variance]}
                    (swap! data update :bars #(take (:bar-count config) %))
                    {:keys [features]} (first bars)
                    vol (Math/sqrt variance)
                    xs (fc-scale-val market features vol price side)
                    row (if verbose? xs [(last xs)])]
                (write-csv writer [row]))))))
      (log/info (format "tested on %s bars" (+ (:bar-count config) @bar-count))))))

(defn run-all []
  (let [target-amts (get-target-amts)]
    (doseq [[market target-amt] target-amts]
      (run market target-amt false))))
