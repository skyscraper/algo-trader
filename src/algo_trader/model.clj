(ns algo-trader.model
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config default-weights scale-alpha]]
            [algo-trader.oms :refer [oms-channels]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [dot-product clip ewm-step epoch]]
            [clojure.core.async :refer [put!]]))

(def bar-count (:bar-count config))
(def scale-target 0.0)
(def scales {})
(def weights (or (:weights config) default-weights))
(def fdm (:fdm config))
(def fc-cap (:scale-cap config))
(def no-result [0.0 false])
(def model-data {})
(def market-info {})

(defn set-scale-target! []
  (alter-var-root #'scale-target (constantly (:scale-target config))))

(defn default-scale [starting-scale]
  (atom {:mean (/ scale-target starting-scale) :scale starting-scale}))

(defn clean-scales []
  (mapv default-scale (:starting-scales config)))

(defn initialize [target-amts mkt-info]
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
    (alter-var-root #'scales merge s-data)
    (alter-var-root #'market-info merge mkt-info)))

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
  [market vol idx ewmac]
  (let [raw-fc (/ ewmac vol) ;; volatility standardization
        scale (update-and-get-forecast-scale! market idx raw-fc)
        scaled-fc (clip fc-cap (* raw-fc scale))]
    scaled-fc))

(defn predict
  "get combined forecast for a market"
  [market {:keys [ewmacs variance]}]
  (let [vol (Math/sqrt variance)
        forecasts (map-indexed
                   (fn [idx ewmac] (predict-single market vol idx ewmac))
                   ewmacs)]
    (->> (dot-product weights forecasts)
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

(defn handle-trade [{:keys [market data]}]
  (let [now (System/currentTimeMillis)
        l (list market)]
    (doseq [{:keys [price time] :as trade} data]
      (statsd/count :trade 1 l)
      (let [trade (update trade :side keyword)
            [forecast real?] (update-and-predict! market trade)
            trade-time (epoch time)
            trade-delay (- now trade-time)]
        (statsd/distribution :trade-delay trade-delay nil)
        (when real?
          (put! (market oms-channels)
                {:msg-type :target
                 :market market
                 :price price
                 :forecast forecast
                 :side (:side trade)
                 :vol (Math/sqrt (:variance @(market model-data)))
                 :ts trade-time}))))))
