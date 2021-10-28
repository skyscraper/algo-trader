(ns algo-trader.model
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config bar-count default-weights scale-alpha]]
            [algo-trader.oms :as oms]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [dot-product clip ewm-step]]
            [clojure.core.async :refer [put!]]))

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
        (let [new-mean (ewm-step mean (Math/abs ^double raw-forecast) scale-alpha)
              new-scale (/ scale-target new-mean)]
          {:mean new-mean :scale new-scale})))))

(defn predict-single
  "get prediction for a single window of a market"
  [market sigma idx x]
  (let [raw-fc (/ x sigma) ;; volatility standardization
        scale (if (:dynamic-scale? config)
                (update-and-get-forecast-scale! market idx raw-fc)
                (get-in config [:starting-scales idx]))
        scaled-fc (clip fc-cap (* raw-fc scale))]
    [raw-fc scale scaled-fc]))

(defn predict
  "get combined forecast for a market"
  [market {:keys [bars]}]
  (let [{:keys [features oi sigma-day]} (first bars)]
    (statsd/gauge :order-imbalance oi [(str "coin" market)])
    (->> features
         (map-indexed
           (fn [idx x]
             (last ;; last of each tuple, see above
               (predict-single market sigma-day idx x))))
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

(defn handle-trade [market {:keys [price side] :as trade}]
  (let [[forecast real?] (update-and-predict! market trade)]
    (when real?
      (put! (market oms/oms-channels)
            {:msg-type :target
             :market   market
             :price    price
             :forecast forecast
             :side     side
             :sigma    (-> model-data market deref :bars first :sigma-day)}))))

(defn fc-scale-val [market features sigma price side]
  (let [xs (map-indexed (fn [idx x] (predict-single market sigma idx x)) features)
        combined (dot-product weights (map last xs))
        fdm-fc (clip fc-cap (* combined fdm))
        port-val (oms/update-paper-port! market price fdm-fc side sigma (market oms/positions))]
    (vec (conj (vec (apply concat xs)) sigma combined fdm-fc port-val))))

(defn evaluate-model
  [market bs record-fn verbose?]
  (doseq [{:keys [features sigma-day c last-side]} bs
          :let [xs (fc-scale-val market features sigma-day c last-side)
                row (if verbose? xs [(last xs)])]]
    (record-fn row)))
