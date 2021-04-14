(ns algo-trader.bars
  (:require [algo-trader.config :refer [config window-alphas fc-window-delta]]
            [algo-trader.utils :refer [ewm-step]]))

(def base {:amt 0.0 :imb 0.0})

(defn bar-base [target-amt]
  {:current base
   :bars '()
   :ewms (repeat (:num-windows config) nil)
   :target-amt target-amt})

;; only keeping track of the fields I care about for now
(defn update-bar [bar {:keys [price size side]}]
  (let [amt (* price size)]
    (-> (assoc bar :c price)
        (update :amt + amt)
        (update :imb (if (= :buy side) + -) amt))))

(defn add-to-bars
  [{:keys [current bars ewms target-amt] :as acc}
   {:keys [price] :as trade}]
  (let [updated (update-bar current trade)]
    (if (and (>= (:amt updated) target-amt)
             (>= (Math/abs (:imb updated)) (* (:imbalance-pct config) target-amt)))
      (let [{:keys [c] :or {c price}} (last bars)
            new-ewms (map #(ewm-step %1 price %2) ewms window-alphas)
            new-ewmacs (map - new-ewms (drop fc-window-delta new-ewms))
            new-bar (assoc updated :diff (- price c))]
        (assoc (update acc :bars conj new-bar)
               :current base
               :ewms new-ewms
               :ewmacs new-ewmacs))
      (assoc acc :current updated))))
