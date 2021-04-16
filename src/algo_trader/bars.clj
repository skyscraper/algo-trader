(ns algo-trader.bars
  (:require [algo-trader.config :refer [config window-alphas fc-window-delta]]
            [algo-trader.utils :refer [ewm-step]]))

(def base {:amt 0.0 :v+ 0.0 :v- 0.0 :imb+ 0.0 :imb- 0.0})

(defn bar-base [target-amt]
  {:current base
   :bars '()
   :ewms (repeat (:num-windows config) nil)
   :target-amt target-amt})

;; only keeping track of the fields I care about for now
(defn update-bar [bar {:keys [price size side]}]
  (let [amt (* price size)
        [v+ v- imb+ imb-] (if (= :buy side) [size 0.0 amt 0.0] [0.0 size 0.0 amt])]
    (-> (update bar :amt + amt)
        (update :v+ + v+)
        (update :v- + v-)
        (update :imb+ + imb+)
        (update :imb- + imb-))))

(defn add-to-bars
  [{:keys [current bars ewms target-amt] :as acc}
   trade]
  (let [{:keys [amt v+ v- imb+ imb-] :as updated} (update-bar current trade)]
    (if (>= amt target-amt)
      (let [vwap1 (if (>= imb+ imb-) (/ imb+ v+) (/ imb- v-))
            str+ (/ imb+ amt)
            str- (/ imb- amt)
            {:keys [vwap] :or {vwap vwap1}} (last bars)
            new-ewms (map #(ewm-step %1 vwap1 %2) ewms window-alphas)
            new-ewmacs (->> (map - new-ewms (drop fc-window-delta new-ewms))
                            (map #(* % (if (neg? %) str- str+))))
            new-bar (assoc updated :vwap vwap1 :diff (- vwap1 vwap))]
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :ewms new-ewms
         :ewmacs new-ewmacs))
      (assoc acc :current updated))))
