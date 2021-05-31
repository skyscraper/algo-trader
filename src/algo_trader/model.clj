(ns algo-trader.model
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config scale-alpha fc-count]]
            [algo-trader.oms :refer [oms-channels]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [clip ewm-step epoch]]
            [clojure.core.async :refer [put!]]
            [clojure.tools.logging :as log]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.modelling :as ds-mod]
            [tech.v3.ml :as ml]
            [tech.v3.libs.xgboost]))

(def bar-count (:bar-count config))
(def scale-target 0.0)
(def scales {})
(def fdm (:fdm config))
(def fc-cap (:scale-cap config))
(def no-result [0.0 false])
(def model-data {})
(def market-info {})
(def models (atom {}))

(defn set-scale-target! []
  (alter-var-root #'scale-target (constantly (:scale-target config))))

(defn default-scale [starting-scale]
  (atom {:mean (/ scale-target starting-scale) :scale starting-scale}))

(defn initialize [target-amts mkt-info]
  (let [m-data (reduce-kv
                (fn [acc market target-amt]
                  (assoc acc market (atom (bars/bar-base target-amt))))
                {}
                target-amts)
        s-data (reduce-kv
                (fn [acc market _]
                  (assoc acc market (default-scale (:starting-scale config))))
                {}
                target-amts)]
    (alter-var-root #'model-data merge m-data)
    (alter-var-root #'scales merge s-data)
    (alter-var-root #'market-info merge mkt-info)))

(defn update-and-get-forecast-scale!
  "update raw forecast scaling values and return latest scale"
  [market raw-forecast]
  (:scale
   (swap!
    (market scales)
    (fn [{:keys [mean]}]
      (let [new-mean (ewm-step mean (Math/abs raw-forecast) scale-alpha)
            new-scale (/ scale-target new-mean)]
        {:mean new-mean :scale new-scale})))))

(def header
  (let [f (fn [i v] (keyword (str v i)))]
    (vec
     (concat (map-indexed f (repeat fc-count "ewmac"))
             (map-indexed f (repeat fc-count "mfi"))))))

(defn row [ewmacs mfis]
  (zipmap header (vec (concat ewmacs mfis))))

(defn model-predict [market ewmacs mfis]
  (let [xs (ds/->dataset [(row ewmacs mfis)])]
    (try
      (:rtn (ml/predict xs (market @models)))
      (catch Exception e
        (log/error "problem with prediction: " (.getMessage e))
        (vec (repeat (last (ds/shape xs)) 0.0))))))

(defn predict
  "get combined forecast for a market"
  [market {:keys [bars]}]
  (let [{:keys [vol ewmacs mfis]} (last bars)
        pred (model-predict market ewmacs mfis)
        raw-fc (/ pred vol) ;; volatility standardization
        scale (update-and-get-forecast-scale! market raw-fc)]
    (->> (* raw-fc scale)
         (clip fc-cap)
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
    (doseq [{:keys [price time] :as trade} data
            :let [trade-time (epoch time)
                  trade-delay (- now trade-time)
                  trade (update trade :side keyword)]]
      (statsd/distribution :trade-delay trade-delay nil)
      (statsd/count :trade 1 l)
      (let [[forecast real?] (update-and-predict! market trade)]
        (when real?
          (put! (market oms-channels)
                {:msg-type :target
                 :market market
                 :price price
                 :forecast forecast
                 :side (:side trade)
                 :vol (Math/sqrt (:variance @(market model-data)))
                 :ts trade-time}))))))

(defn feature-dataset [bars]
  (-> (reduce
       (fn [acc [{:keys [ewmacs mfis]} {:keys [rtn]}]]
         (conj acc (assoc (row ewmacs mfis) :rtn rtn)))
       []
       (partition 2 1 bars))
      ds/->dataset
      (ds-mod/set-inference-target :rtn)))

(defn get-model [dataset]
  (ml/train-split dataset {:model-type :xgboost/regression}))

(defn generate-models
  [target-amts trades]
  (doseq [[market target-amt] target-amts
          :let [bs (bars/generate-bars target-amt trades)
                dataset (feature-dataset bs)
                m (get-model dataset)]]
    (swap! models assoc market m)))
