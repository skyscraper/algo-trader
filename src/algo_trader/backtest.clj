(ns algo-trader.backtest
  (:require [algo-trader.api :as api]
            [algo-trader.bars :as bars]
            [algo-trader.config :refer [config]]
            [algo-trader.core :as core]
            [algo-trader.db :as db]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.utils :refer [uc-kw clip]]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]))

(defn fc-scale-val [market ewmacs mfis vol price side]
  (let [pred (first (model/model-predict market ewmacs mfis))
        raw-fc (/ pred vol)
        scale (model/update-and-get-forecast-scale! market raw-fc)
        scaled-fc (clip model/fc-cap (* raw-fc scale))
        fdm-fc (clip model/fc-cap (* scaled-fc model/fdm))
        window-val (oms/update-port! price fdm-fc side vol (market oms/positions))]
    [pred vol raw-fc scaled-fc fdm-fc scale window-val]))

(def header ["pred" "vol" "raw-fc" "scaled-fc" "fdm-fc" "scale" "port-val"])
(defn run
  "run a backtest for a single market, outputting portfolio prices for each window"
  [underlying training-percent verbose?]
  (let [market (uc-kw (str (name underlying) "-PERP"))
        m-list [market]
        u-set #{underlying}
        target-amts (api/get-futures-targets u-set)]
    (log/info (format "starting backtest for %s" (name underlying)))
    (model/set-scale-target!)
    (let [market-info (api/get-market-info u-set)]
      (model/initialize target-amts market-info)
      (reset! core/markets (keys target-amts)))
    (oms/initialize-equity (:test-market-notional config))
    (let [starting-cash (oms/determine-notionals @core/markets)]
      (log/info (format "starting equity per market: %,.2f" starting-cash))
      (oms/initialize-positions m-list starting-cash))
    (log/info "fetching trades...")
    (let [bar-count (atom 0)
          begin (db/get-first-ts)
          end (db/get-last-ts)
          split-ts (+ begin (* training-percent (- end begin)))
          training-data (db/get-trades begin split-ts)
          backtest-trades (db/get-trades split-ts end)
          data (market model/model-data)]
      (log/info "generating models...")
      (model/generate-models target-amts training-data)
      (log/info (format "processing %s trades..." (count backtest-trades)))
      (with-open [writer (io/writer "resources/backtest.csv")]
        (write-csv writer (if verbose? [header] [["port-val"]]))
        (doseq [trade backtest-trades]
          (let [{:keys [price side] :as trade} (update trade :side keyword)]
            (swap! data bars/add-to-bars trade)
            (when (> (count (:bars @data)) (:bar-count config))
              (swap! bar-count inc)
              (let [{:keys [bars]}
                    (swap! data update :bars #(take (:bar-count config) %))
                    {:keys [vol ewmacs mfis]} (last bars)
                    xs (fc-scale-val market ewmacs mfis vol price side)
                    row (if verbose? xs [(last xs)])]
                (write-csv writer [row]))))))
      (log/info (format "created %s bars" (+ (:bar-count config) @bar-count))))))

