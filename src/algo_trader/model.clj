(ns algo-trader.model
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config scale-alpha fc-count]]
            [algo-trader.db :as db]
            [algo-trader.oms :as oms]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [pct-rtn clip ewm-step epoch get-target-amts]]
            [clojure.core.async :refer [put!]]
            [clojure.tools.logging :as log]
            [java-time :refer [as duration]]
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
(def models (atom {}))

(defn set-scale-target! []
  (alter-var-root #'scale-target (constantly (:scale-target config))))

(defn default-scale [starting-scale]
  (atom {:mean (/ scale-target starting-scale) :scale starting-scale}))

(defn model-fname [market]
  (format "resources/model_%s.npy" (name market)))

(defn freeze-model [market m]
  (let [model (-> (update m :model-data dissoc :metrics)
                  (update :options dissoc :watches))]
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

(defn initialize [target-amts]
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
    (alter-var-root #'scales merge s-data)))

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
             (map-indexed f (repeat fc-count "rsi"))
             [:mpb]))))

(defn row [features]
  (zipmap header features))

(defn model-predict [model features]
  (let [xs (ds/->dataset [(row features)])]
    (try
      (:diff (ml/predict xs model))
      (catch Exception e
        (log/error "problem with prediction: " (.getMessage e))
        (vec (repeat (last (ds/shape xs)) 0.0))))))

(defn predict
  "get combined forecast for a market"
  [market {:keys [bars variance]}]
  (let [{:keys [features]} (first bars)
        pred (first (model-predict (market @models) features))
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
          (put! (market oms/oms-channels)
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
       (take 1000)
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

(defn generate-model
  [dataset]
  (let [train-test-split (ds-mod/train-test-split dataset)
        model-options (gridsearch-options train-test-split)]
    (get-model train-test-split model-options)))

(defn fc-scale-val [market model features vol price side]
  (let [pred (first (model-predict model features))
        raw-fc (/ pred vol)
        scale (update-and-get-forecast-scale! market raw-fc)
        scaled-fc (clip fc-cap (* raw-fc scale))
        fdm-fc (clip fc-cap (* scaled-fc fdm))
        port-val (oms/update-paper-port! price fdm-fc side vol (market oms/positions))]
    [pred raw-fc scaled-fc fdm-fc scale port-val]))

(defn evaluate-model
  [market model bs record-fn verbose?]
  (doseq [{:keys [features sigma c last-side]} bs
          :let [xs (fc-scale-val market model features sigma c last-side)
                row (if verbose? xs [(last xs)])]]
    (record-fn row)))

(defn perf-search [market keep?]
  (log/info "starting perf search")
  (set-scale-target!)
  (oms/initialize-equity (:test-market-notional config))
  (let [end (db/get-last-ts market)
        begin (- end (as (duration (:total-days config) :days) :millis))
        split-ts (long (+ begin (* (:training-split config) (- end begin))))
        target-amt (market (get-target-amts))
        training-ds (->> (db/get-trades market begin split-ts)
                         (bars/generate-bars target-amt)
                         feature-dataset)
        test-bars (->> (db/get-trades market split-ts end)
                       (bars/generate-bars target-amt)
                       reverse)
        x (atom (or (thaw-model market) {:rtn 0.0 :sharpe 0.0}))]
    (doseq [m (repeatedly 10 #(generate-model training-ds))]
      (log/info "starting next loop...")
      (let [a (atom [(:test-market-notional config)])
            record-fn (fn [[x]] (swap! a conj x))]
        (initialize {market target-amt})
        (oms/initialize-positions [market] (:test-market-notional config))
        (evaluate-model market m test-bars record-fn false)
        (let [total-rtn (pct-rtn (first @a) (last @a))
              rtns (map pct-rtn @a (rest @a))
              n (count rtns)
              mu (/ (apply + rtns) n)
              sigma (Math/sqrt
                     (/ (apply + (map #(Math/pow (- % mu) 2) rtns))
                        n))
              sharpe (/ mu sigma)]
          (swap! x (fn [y]
                     (if (> sharpe (:sharpe y 0.0))
                       (assoc m :rtn total-rtn :sharpe sharpe)
                       y)))
          (log/info (format "mu: %f sharpe %f" mu sharpe)))))
    (when keep?
      (swap! models assoc market @x)
      (freeze-model market @x))
    @x))

;; convenience for testing
(defn sample-dataset [market]
  (let [end (db/get-last-ts market)
        begin (- end (as (duration (:total-days config) :days) :millis))
        split-ts (long (+ begin (* (:training-split config) (- end begin))))
        trades (db/get-trades market begin split-ts)
        target-amt (market (get-target-amts))
        bs (bars/generate-bars target-amt trades)]
    (feature-dataset bs)))
