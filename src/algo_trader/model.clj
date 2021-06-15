(ns algo-trader.model
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config scale-alpha fc-count]]
            [algo-trader.db :as db]
            [algo-trader.oms :refer [oms-channels]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [clip ewm-step epoch get-target-amts]]
            [clojure.core.async :refer [put!]]
            [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.modelling :as ds-mod]
            [tech.v3.libs.smile.regression]
            [tech.v3.libs.xgboost]
            [tech.v3.ml.loss :as loss]
            [tech.v3.ml :as ml]
            [tech.v3.ml.gridsearch :as gs]))

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

(defn model-fname [market]
  (format "../resources/model_%s.npy" (name market)))

(defn freeze-model [market m]
  (let [model [(-> (update m :model-data dissoc :metrics)
                   (update :options dissoc :watches))]]
    (nippy/freeze-to-file (model-fname market) model)))

(defn thaw-model [market]
  (try
    (nippy/thaw-from-file (model-fname market))
    (catch Exception _ nil)))

(defn load-models [markets]
  (doseq [market markets
          :let [m (thaw-model market)]]
    (when m
      (log/info "found model for:" market)
      (swap! models assoc market m))))

(defn freeze-models []
  (doseq [[market m] @models]
    (when m
      (freeze-model market m))))

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
    (alter-var-root #'market-info merge mkt-info)
    (load-models (keys target-amts))))

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
             (map-indexed f (repeat (:lag config) "voi"))
             (map-indexed f (repeat (:lag config) "oir"))
             [:mpb]))))

(defn row [features]
  (zipmap header features))

(defn model-predict [market features]
  (let [xs (ds/->dataset [(row features)])]
    (try
      (:diff (ml/predict xs (market @models)))
      (catch Exception e
        (log/error "problem with prediction: " (.getMessage e))
        (vec (repeat (last (ds/shape xs)) 0.0))))))

(defn predict
  "get combined forecast for a market"
  [market {:keys [bars variance]}]
  (let [{:keys [features]} (first bars)
        pred (first (model-predict market features))
        raw-fc (/ pred (Math/sqrt variance))
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
       (fn [acc bars]
         (let [[{:keys [features]} & bs] (reverse bars)
               diffs (map :diff bs)
               cumulative-diffs (rest (reductions + 0.0 diffs))
               avg (/ (apply + cumulative-diffs) (:forecast-window config))]
           (conj acc (assoc (row features) :diff avg))))
       []
       (partition (inc (:forecast-window config)) 1 bars))
      ds/->dataset
      (ds-mod/set-inference-target :diff)))

(defn gridsearchable-options [model-type]
  (merge {:model-type model-type} (ml/hyperparameters model-type)))

(defn test-options
  [train-test-split options]
  (let [model (ml/train (:train-ds train-test-split) options)
        prediction (ml/predict (:test-ds train-test-split) model)]
    (assoc model :loss (loss/mae (prediction :diff)
                                 ((:test-ds train-test-split) :diff)))))

(defn gridsearch-options [train-test-split]
  (->> (gridsearchable-options (:model-type config))
       (gs/sobol-gridsearch)
       (take 100)
       (map #(test-options train-test-split %))
       (sort-by :loss)
       first ;; could potentially iterate on this more in the future...
       :options))

(defn get-model [train-test-split model-options]
  (ml/train (:train-ds train-test-split)
            (assoc model-options
                   :watches {:test-ds (:test-ds train-test-split)}
                   :eval-metric "mae"
                   :round 100
                   :early-stopping-round 4)))

(defn generate-models
  [target-amts trades]
  (doseq [[market target-amt] target-amts
          :let [bs (bars/generate-bars target-amt trades)
                dataset (feature-dataset bs)
                train-test-split (ds-mod/train-test-split dataset)
                model-options (gridsearch-options train-test-split)
                m (get-model train-test-split model-options)]]
    (swap! models assoc market m)))

;; convenience for testing
(defn sample-dataset [market]
  (let [begin (db/get-first-ts)
        end (db/get-last-ts)
        split-ts (long (+ begin (* 2/3 (- end begin))))
        trades (db/get-trades begin split-ts)
        target-amt (market (get-target-amts))
        bs (bars/generate-bars target-amt trades)]
    (feature-dataset bs)))
