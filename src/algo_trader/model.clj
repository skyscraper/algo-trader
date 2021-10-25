(ns algo-trader.model
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config default-weights scale-alpha]]
            [algo-trader.oms :as oms]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [dot-product clip ewm-step]]
            [clojure.core.async :refer [put!]]))

(def bar-count (:bar-count config))
(def scale-target (:scale-target config))
(def scales {})
(def weights (or (:weights config) default-weights))
(def fdm (:fdm config))
(def fc-cap (:scale-cap config))
(def no-result [0.0 false])
(def model-data {})

(defn default-scale [starting-scale]
  (atom {:mean (/ scale-target starting-scale) :scale starting-scale}))

(defn clean-scales []
  (mapv default-scale (:starting-scales config)))

(defn initialize [target-amts]
  (let [m-data (reduce-kv
                 (fn [acc market target-amt]
                   (assoc acc market (atom (bars/bar-base target-amt))))
                 {}
                 target-amts)
        s-data (reduce-kv
                 (fn [acc market _] (assoc acc market (clean-scales)))
                 {}
                 target-amts)]
    (alter-var-root #'model-data merge m-data)
    (alter-var-root #'scales merge s-data)))

(defn update-and-get-forecast-scale!
  "update raw forecast scaling values and return latest scale"
  [market idx raw-forecast]
  (:scale
    (swap!
      (get-in scales [market idx])
      (fn [{:keys [mean]}]
        (let [new-mean (ewm-step mean (Math/abs raw-forecast) scale-alpha)
              new-scale (/ scale-target new-mean)]
          {:mean new-mean :scale new-scale})))))

(defn predict-single
  "get prediction for a single window of a market"
  [market vol idx x]
  (let [raw-fc (/ x vol)                                    ;; volatility standardization
        scale (update-and-get-forecast-scale! market idx raw-fc)
        scaled-fc (clip fc-cap (* raw-fc scale))]
    [raw-fc scale scaled-fc]))

(defn predict
  "get combined forecast for a market"
  [market {:keys [bars variance]}]
  (let [{:keys [features twobv v]} (first bars)
        vol (Math/sqrt variance)]
    (statsd/gauge :order-imbalance (- (/ twobv v) 1.0) [market])
    (->> features
         (map-indexed
           (fn [idx x]
             (last                                          ;; last of each tuple, see above
               (predict-single market vol idx x))))
         (dot-product weights)
         (* fdm)
         (clip fc-cap))))

(defn update-and-predict!
  "update model data and generate prediction"
  [market trade]
  (if-let [data (market model-data)]
    (do
      (swap! data bars/add-to-bars trade)
      (if (> (count (:bars @data)) bar-count)
        [(predict market (swap! data update :bars #(take bar-count %))) true]
        no-result))
    no-result))

(defn handle-trade [market {:keys [price time] :as trade}]
  (let [[forecast real?] (update-and-predict! market trade)]
    (when real?
      (put! (market oms/oms-channels)
            {:msg-type :target
             :market   market
             :price    price
             :forecast forecast
             :side     (:side trade)
             :vol      (Math/sqrt (:variance @(market model-data)))
             :ts       time}))))

(defn fc-scale-val [market features vol price side]
  (let [xs (map-indexed (fn [idx x] (predict-single market vol idx x)) features)
        combined (dot-product weights (map last xs))
        fdm-fc (clip fc-cap (* combined fdm))
        port-val (oms/update-paper-port! price fdm-fc side vol (market oms/positions))]
    (vec (conj (vec (apply concat xs)) combined fdm-fc port-val))))

(defn evaluate-model
  [market bs record-fn verbose?]
  (doseq [{:keys [features sigma c last-side]} bs
          :let [xs (fc-scale-val market features sigma c last-side)
                row (if verbose? xs [(last xs)])]]
    (record-fn row)))
