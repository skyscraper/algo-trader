(ns algo-trader.backtest
  (:require [algo-trader.api :as api]
            [algo-trader.bars :as bars]
            [algo-trader.config :refer [config fc-count]]
            [algo-trader.core :as core]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.utils :refer [uc-kw clip]]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]))

(defn fc-scale-val [market idx ewmac mfi vol price side]
  (let [pred (first (model/model-predict market idx ewmac mfi))
        raw-fc (/ pred vol)
        scale (model/update-and-get-forecast-scale! market idx raw-fc)
        scaled-fc (clip model/fc-cap (* raw-fc scale))
        fdm-fc (clip model/fc-cap (* scaled-fc model/fdm))
        window-val (oms/update-port! price fdm-fc side vol (market oms/positions))]
    [pred vol raw-fc scaled-fc fdm-fc scale window-val]))

(def header ["pred" "vol" "raw-fc" "scaled-fc" "fdm-fc" "scale" "port-val"])
(defn run
  "run a backtest for a single market, outputting portfolio prices for each window"
  [underlying test-time lookback-days verbose?]
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
          ts (or test-time (long (/ (System/currentTimeMillis) 1000)))
          trades (api/get-db-trades) ;; todo: maybe make this configurable?
          data (market model/model-data)]
      (log/info "generating models...")
      (model/generate-models target-amts trades)
      (log/info (format "processing %s trades..." (count trades)))
      (with-open [writer (io/writer "resources/backtest.csv")]
        (write-csv
         writer
         (if verbose?
           [(mapcat
             (fn [i]
               (map #(str % i) header))
             (range fc-count))]
           [(map #(str "port-val") (range fc-count))]))
        (doseq [trade trades]
          (let [{:keys [price side] :as trade} (update trade :side keyword)]
            (swap! data bars/add-to-bars trade)
            (when (> (count (:bars @data)) (:bar-count config))
              (swap! bar-count inc)
              (let [{:keys [bars]}
                    (swap! data update :bars #(take (:bar-count config) %))
                    {:keys [vol ewmacs mfis]} (last bars)
                    xs (mapv
                        (fn [idx]
                          (fc-scale-val market idx (nth ewmacs idx)
                                        (nth mfis idx) vol price side))
                        (range fc-count))
                    row (if verbose?
                          (apply concat xs)
                          (map last xs))]
                (write-csv writer [row]))))))
      (log/info (format "created %s bars" (+ (:bar-count config) @bar-count))))))

