(ns algo-trader.bars
  (:require [algo-trader.config :refer [config window-alphas fc-window-delta]]
            [algo-trader.utils :refer [ewm-step]]))

(def base {:amt 0.0 :v 0.0 :imb 0.0})

(defn bar-base [target-amt]
  {:current base
   :bars '()
   :ewms (repeat (:num-windows config) nil)
   :target-amt target-amt})

(defn update-bar [bar {:keys [price size side]}]
  (let [amt (* price size)
        op (if (= :buy side) + -)]
    (-> (update bar :amt + amt)
        (update :v + size)
        (update :imb op amt))))

(defn add-to-bars
  [{:keys [current bars ewms target-amt] :as acc}
   trade]
  (let [{:keys [amt v] :as updated} (update-bar current trade)]
    (if (>= amt target-amt)
      (let [vwap1 (/ amt v)
            {:keys [vwap] :or {vwap vwap1}} (last bars)
            new-ewms (map #(ewm-step %1 vwap1 %2) ewms window-alphas)
            new-ewmacs (map - new-ewms (drop fc-window-delta new-ewms))
            new-bar (assoc updated :vwap vwap1 :diff (- vwap1 vwap))]
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :ewms new-ewms
         :ewmacs new-ewmacs))
      (assoc acc :current updated))))
