(ns algo-trader.bars
  (:require [algo-trader.config :refer [bar-count fc-count vol-scale-daily vol-alpha window-alphas]]
            [algo-trader.utils :refer [pct-rtn ewm-step]]))

(def base {:v 0.0 :buys 0 :sells 0 :bv 0.0 :sv 0.0})

(def feature-base {:ewms (vec (repeat fc-count nil))})

(defn imbalance [{:keys [bv sv v]}]
  (/ (- bv sv) v))

(defn bar-base [target-size signal-channel]
  {:current        base
   :bars           '()
   :features-data  feature-base
   :target-size    target-size
   :signal-channel signal-channel})

(defn update-bar [bar {:keys [price size side]}]
  (-> (update bar :o (fnil identity price))
      (update :h (fnil max price) price)
      (update :l (fnil min price) price)
      (assoc :c price)
      (update :v + size)
      (update :buys (if (= side :buy) inc identity))
      (update :sells (if (= side :sell) inc identity))
      (update :bv + (if (= side :buy) size 0.0))
      (update :sv + (if (= side :sell) size 0.0))))

(defn add-to-bars
  [{:keys [current bars features-data variance target-size] :as acc}
   {:keys [price side] :as trade}]
  (let [{:keys [o bv sv v] :as updated} (update-bar current trade)
        oi (imbalance updated)]
    (if (>= v target-size)
      (let [prev-close (:c (first bars) o)
            {:keys [ewms]} features-data
            ;; ewm-oi
            new-ewms (mapv #(ewm-step %1 oi %2) ewms window-alphas)
            rtn (pct-rtn prev-close price)
            sq-rtn (Math/pow rtn 2.0)
            new-variance (ewm-step variance sq-rtn vol-alpha)
            sigma (Math/sqrt new-variance)
            sigma-day (* sigma vol-scale-daily)
            new-bar (assoc updated
                           :last-side (cond
                                        (pos? oi) :buy
                                        (neg? oi) :sell
                                        :else side)
                           :features new-ewms
                           :sigma sigma
                           :sigma-day sigma-day)]
        ;; todo: check for signal and if so, send on channel
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :features-data {:ewms new-ewms}
         :variance new-variance))
      (let []
        ;; todo: check oi against ewm oi of last bars (subject to min volume?)
        ;; todo: check max tick run against ewm of max tick run (either direction)
        ;; generate signal separate from bar...how?
        (assoc acc :current updated)))))

(defn generate-bars [target-size signal-channel trades]
  (->> (reduce
        add-to-bars
        (bar-base target-size signal-channel)
        trades)
       :bars
       (drop-last bar-count)))
