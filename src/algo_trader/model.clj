(ns algo-trader.model
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config fc-count scale-alpha default-weights]]
            [algo-trader.oms :refer [oms-channels]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [dot-product ewm-vol ewm-step epoch]]
            [clojure.core.async :refer [put!]]))

(def bar-count (:bar-count config))
(def scale-target 0.0)
(def scales {})
(def weights (:weights config))
(def no-result [0.0 false])
(def model-data {})

(defn set-scale-target [max-pos]
  (alter-var-root #'scale-target (constantly (* (:scale-target config) max-pos))))

(defn default-scale []
  (atom {:mean nil :scale nil}))

(defn clean-scales []
  (vec (repeatedly fc-count default-scale)))

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
  [market vol idx ewmac]
  (let [raw-fc (/ ewmac vol) ;; current condition scaling
        scale (update-and-get-forecast-scale! market idx raw-fc)]
    (/ (* raw-fc scale) vol))) ;; scale AND divide by sigma again

(defn predict
  "get combined prediction for a market"
  [market {:keys [ewmacs bars]}]
  (let [vol (ewm-vol (map :diff bars))
        forecasts (map-indexed
                   (fn [idx ewmac] (predict-single market vol idx ewmac))
                   ewmacs)]
    (dot-product forecasts (market weights default-weights)))) ;; portfolio weightings

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
            [target real?] (update-and-predict! market trade)
            trade-time (epoch time)
            trade-delay (- now trade-time)]
        (statsd/distribution :trade-delay trade-delay nil)
        (when real?
          (put! (market oms-channels)
                {:msg-type :target
                 :market market
                 :price price
                 :target target
                 :side (:side trade)
                 :ts trade-time}))))))
