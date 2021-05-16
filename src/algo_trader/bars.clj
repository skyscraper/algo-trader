(ns algo-trader.bars
  (:require [algo-trader.config :refer [config vol-alpha window-alphas fc-window-delta]]
            [algo-trader.utils :refer [pct-rtn ewm-step]]))

(def base {:amt 0.0 :v 0.0 :imb 0.0})

(defn bar-base [target-amt]
  {:current base
   :bars '()
   :ewms (repeat (:num-windows config) nil)
   :target-amt target-amt})

(defn update-bar [bar {:keys [price size side]}]
  (let [amt (* price size)
        op (if (= :buy side) + -)]
    (-> (assoc bar :c price)
        (update :amt + amt)
        (update :v + size)
        (update :imb op amt))))

(defn add-to-bars
  [{:keys [current bars ewms variance target-amt] :as acc}
   {:keys [price] :as trade}]
  (let [{:keys [amt] :as updated} (update-bar current trade)]
    (if (>= amt target-amt)
      (let [{:keys [c] :or {c price}} (last bars)
            new-ewms (map #(ewm-step %1 price %2) ewms window-alphas)
            new-ewmacs (map - new-ewms (drop fc-window-delta new-ewms))
            rtn (pct-rtn c price)
            sq-rtn (Math/pow rtn 2.0)
            new-variance (ewm-step variance sq-rtn vol-alpha)]
        (assoc
         (update acc :bars conj updated)
         :current base
         :ewms new-ewms
         :ewmacs new-ewmacs
         :variance new-variance))
      (assoc acc :current updated))))
