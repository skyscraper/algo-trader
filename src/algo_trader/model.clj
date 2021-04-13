(ns algo-trader.model
  (:require [algo-trader.bars :as bars]
            [algo-trader.config :refer [config fc-count scale-alphas]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [dot-product ewm-vol ewm-step epoch]]
            [clojure.tools.logging :as log]))

(def bar-count (:bar-count config))
(def scale-target (atom 0.0))
(defn default-scale []
  (atom {:mean nil :scale nil}))
(defn clean-scales []
  (vec (repeatedly fc-count default-scale)))
(def scales (reduce
             (fn [m k] (assoc m k (clean-scales)))
             {}
             (:pairs config)))
(def weights (:weights config))
(def no-result [0.0 false])
(def model-data {})

(defn set-scale-target [max-pos]
  (reset! scale-target (* (:scale-target-percent config) max-pos)))

(defn initialize [target-amts]
  (doseq [[pair target] target-amts]
    (alter-var-root #'model-data assoc pair (atom (bars/bar-base target)))))

(defn update-forecast-scale!
  "updates ewm values for forecast scaling"
  [idx scale-data forecast scale-target]
  (let [alpha (nth scale-alphas idx)]
    (swap! scale-data
           (fn [{:keys [mean]}]
             (let [new-mean (ewm-step mean (Math/abs forecast) alpha)
                   new-scale (/ scale-target new-mean)]
               {:mean new-mean :scale new-scale})))))

(defn predict [market ewmacs rtns]
  (let [vol (ewm-vol rtns)
        unscaled (map #(/ % vol) ewmacs) ;; current condition scaling
        scaled (doall ;; eagerly scale & update ewm
                (map-indexed
                 (fn [idx fc]
                   (let [scale-data (get-in scales [market idx])
                         scale (:scale @scale-data)]
                     (update-forecast-scale! idx scale-data fc @scale-target)
                     ;; scale AND divide by sigma again
                     (/ (* fc (or scale (:scale @scale-data))) vol)))
                 unscaled))]
    (dot-product scaled (market weights)))) ;; portfolio weightings

(defn update-and-predict! [market trade]
  (if-let [data (market model-data)]
    (do
      (swap! data bars/add-to-bars trade)
      (if (> (count (:bars @data)) bar-count)
        (do
          (swap! data update :bars #(take bar-count %))
          (let [target (predict market (:ewmacs @data) (map :rtn (:bars @data)))]
            [target true]))
        no-result))
    no-result))



(defn handle-trade [{:keys [market data]}]
  (let [now (System/currentTimeMillis)
        l (list market)]
    (doseq [{:keys [time] :as trade} data]
      (statsd/count :trade 1 l)
      (let [[target real?] (update-and-predict! market (update trade :side keyword))
            trade-delay (- now (epoch time))]
        (statsd/distribution :trade-delay trade-delay nil)
        (when real?
          (log/info (format "SIGNAL: %s target notional position: %s @ %s" market target now))
          ;; todo: send target to oms channel
          )))))
