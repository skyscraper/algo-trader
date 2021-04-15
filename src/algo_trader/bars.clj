(ns algo-trader.bars
  (:require [algo-trader.config :refer [config window-alphas fc-window-delta vol-alpha]]
            [algo-trader.utils :refer [ewm-step]]))

(def base {:amt 0.0 :imb 0.0 :last-imb 0.0 :ticks 0})

(def bar-base
  {:current base
   :bars '()
   :ewms (repeat (:num-windows config) nil)
   :t 1
   :bv 0.0})

;; only keeping track of the fields I care about for now
(defn update-bar [bar {:keys [price size side]}]
  (let [amt (* price size)
        imb ((if (= :buy side) + -) amt)]
    (-> (assoc bar :c price)
        (update :amt + amt)
        (update :imb + imb)
        (assoc :last-imb imb)
        (update :ticks inc))))

(defn add-to-bars
  [{:keys [current bars ewms t bv] :as acc}
   {:keys [price] :as trade}]
  (let [{:keys [ticks last-imb] :as updated} (update-bar current trade)]
    (if (>= (Math/abs (:imb updated)) (Math/abs (* t bv)))
      (let [{:keys [c] :or {c price}} (last bars)
            new-ewms (map #(ewm-step %1 price %2) ewms window-alphas)
            new-ewmacs (map - new-ewms (drop fc-window-delta new-ewms))
            new-bar (assoc updated :diff (- price c))]
        (-> (update acc :bars conj new-bar)
            (update :bv ewm-step last-imb vol-alpha)
            (update :t ewm-step ticks vol-alpha)
            (assoc
             :current base
             :ewms new-ewms
             :ewmacs new-ewmacs)))
      (-> (update acc :bv ewm-step last-imb vol-alpha)
          (assoc :current updated)))))
