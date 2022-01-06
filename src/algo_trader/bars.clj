(ns algo-trader.bars
  (:require [algo-trader.config :refer [bar-count vol-scale-daily vol-alpha oi-alpha tr-alpha]]
            [algo-trader.utils :refer [pct-rtn ewm-step]]
            [clojure.core.async :refer [close! put!]]))

(def base {:v 0.0 :buys 0 :sells 0 :bv 0.0 :sv 0.0})

(defn imbalance [{:keys [bv sv v]}]
  (/ (- bv sv) v))

(defn tick-run [{:keys [buys sells]}]
  (max buys sells))

(defn bar-base [target-size signal-channel]
  {:current        base
   :bars           '()
   :features-data  {}
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

(defn signal-check
  [{:keys [current bars features-data target-size signal-channel]}
   {:keys [price side]}]
  (when-not (empty? features-data)
    (let [{:keys [abs-oi-mu abs-oi-variance tr-mu tr-variance]} features-data
          {:keys [v buys sells]} current
          oi-signal (if (> v (/ target-size 2)) ;; must see at least half-bar before checking oi
                      (let [oi (imbalance current)]
                        (if (> (Math/abs oi) (+ abs-oi-mu (Math/sqrt abs-oi-variance)))
                          (if (pos? oi) 1 -1)
                          0))
                      0)
          tr (tick-run current)
          tr-signal (if (> tr (+ tr-mu (Math/sqrt tr-variance)))
                      (if (> buys sells) 1 -1)
                      0)]
      (when (or (not (zero? oi-signal)) (not (zero? tr-signal)))
        (put! signal-channel {:price price
                              :side side
                              :features [oi-signal tr-signal]
                              :sigma (:sigma-day (first bars))})))))

(defn add-to-bars
  [{:keys [current bars features-data variance target-size] :as acc}
   {:keys [price side] :as trade}]
  (let [{:keys [o v] :as updated} (update-bar current trade)
        oi (imbalance updated)]
    (signal-check acc trade)
    (if (>= v target-size)
      (let [prev-close (:c (first bars) o)
            {:keys [abs-oi-mu abs-oi-variance tr-mu tr-variance]} features-data
            ;; rtn/vol
            rtn (pct-rtn prev-close price)
            sq-rtn (Math/pow rtn 2.0)
            new-variance (ewm-step variance sq-rtn vol-alpha)
            sigma (Math/sqrt new-variance)
            sigma-day (* sigma vol-scale-daily)
            ;; new bar
            new-bar (assoc updated
                           :last-side (cond
                                        (pos? oi) :buy
                                        (neg? oi) :sell
                                        :else side)
                           :sigma sigma
                           :sigma-day sigma-day)
            tr (tick-run updated)]
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :features-data {:abs-oi-mu (ewm-step abs-oi-mu (Math/abs oi) oi-alpha)
                         :abs-oi-variance (ewm-step abs-oi-variance (Math/pow oi 2) oi-alpha)
                         :tr-mu (ewm-step tr-mu tr tr-alpha)
                         :tr-variance (ewm-step tr-variance (Math/pow tr 2) tr-alpha)}
         :variance new-variance))
      (assoc acc :current updated))))

(defn generate-bars [target-size signal-channel trades]
  (let [bs (->> (reduce
                 add-to-bars
                 (bar-base target-size signal-channel)
                 trades)
                :bars
                (drop-last bar-count)
                doall)]
    (close! signal-channel)
    bs))
