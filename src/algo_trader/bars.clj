(ns algo-trader.bars
  (:require [algo-trader.config :refer [bar-count fc-count vol-scale-daily vol-alpha window-alphas]]
            [algo-trader.utils :refer [pct-rtn ewm-step]]
            [taoensso.timbre :as log]))

(def base {:v 0.0 :twobv 0.0})

(def feature-base {:ewms (vec (repeat fc-count nil))})

(defn bar-base [target-size]
  {:current       base
   :bars          '()
   :features-data feature-base
   :target-size   target-size})

(defn update-bar [bar {:keys [price size side source]}]
  (-> (update bar :o (fnil identity price))
      (update :h (fnil max price) price)
      (update :l (fnil min price) price)
      (assoc :c price)
      (update :v + size)
      (update :twobv + (* size (condp = side
                                 :buy 2.0
                                 :sell 0.0
                                 (do
                                   (log/warn "unknown side:" side source)
                                   0.0))))))

(defn add-to-bars
  [{:keys [current bars features-data variance target-size] :as acc}
   {:keys [price side] :as trade}]
  (let [{:keys [o twobv v] :as updated} (update-bar current trade)]
    (if (>= v target-size)
      (let [prev-close (:c (first bars) o)
            {:keys [ewms]} features-data
            ;; ewm-oi
            oi (- (/ twobv v) 1.0)
            new-ewms (mapv #(ewm-step %1 oi %2) ewms window-alphas)
            rtn (pct-rtn prev-close price)
            sq-rtn (Math/pow rtn 2.0)
            new-variance (ewm-step variance sq-rtn vol-alpha)
            sigma (Math/sqrt new-variance)
            sigma-day (* sigma vol-scale-daily)
            new-bar (assoc updated
                           :oi oi
                           :last-side (cond
                                        (pos? oi) :buy
                                        (neg? oi) :sell
                                        :else side)
                           :features new-ewms
                           :sigma sigma
                           :sigma-day sigma-day)]
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :features-data {:ewms new-ewms}
         :variance new-variance))
      (assoc acc :current updated))))

(defn generate-bars [target-size trades]
  (->> (reduce
        add-to-bars
        (bar-base target-size)
        trades)
       :bars
       (drop-last bar-count)))
