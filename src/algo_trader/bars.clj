(ns algo-trader.bars
  (:require [algo-trader.config :refer [config vol-alpha window-alphas fast-alphas
                                        fc-count fc-window-delta]]
            [algo-trader.utils :refer [pct-rtn ewm-step]]))

(def base {:amt 0.0 :v 0.0 :pmf 0.0 :nmf 0.0})

(defn bar-base [target-amt]
  {:current base
   :bars '()
   :ewms (repeat (:num-windows config) nil)
   :pmfs (repeat fc-count nil)
   :nmfs (repeat fc-count nil)
   :target-amt target-amt})

(defn update-bar [bar {:keys [price size side]}]
  (let [amt (* price size)
        b? (= :buy side)]
    (-> (assoc bar :c price)
        (update :amt + amt)
        (update :v + size)
        (update :pmf + (if b? amt 0))
        (update :nmf + (if b? 0 amt)))))

(defn add-to-bars
  [{:keys [current bars ewms pmfs nmfs mean variance target-amt] :as acc}
   {:keys [price] :as trade}]
  (let [{:keys [amt pmf nmf] :as updated} (update-bar current trade)]
    (if (>= amt target-amt)
      (let [{:keys [c vol] :or {c price}} (first bars)
            new-ewms (mapv #(ewm-step %1 price %2) ewms window-alphas)
            new-ewmacs (mapv - new-ewms (drop fc-window-delta new-ewms))
            new-pmfs (mapv #(ewm-step %1 pmf %2) pmfs fast-alphas)
            new-nmfs (mapv #(ewm-step %1 nmf %2) nmfs fast-alphas)
            new-mfis (mapv #(- 50.0 (/ 100 (inc (/ %1 %2)))) new-pmfs new-nmfs)
            rtn (pct-rtn c price)
            new-mean (ewm-step mean rtn vol-alpha)
            sq-rtn (Math/pow rtn 2.0)
            new-variance (ewm-step variance sq-rtn vol-alpha)
            new-vol (Math/sqrt new-variance)
            prev-mean (or mean new-mean)
            prev-vol (or vol new-vol)
            phi (if (zero? prev-vol)
                  0.0
                  (/ (- rtn prev-mean) prev-vol))
            new-bar (assoc updated
                           :ewmacs new-ewmacs
                           :mfis new-mfis
                           :phi phi
                           :vol new-vol)]
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :ewms new-ewms
         :pmfs new-pmfs
         :nmfs new-nmfs
         :mean new-mean
         :variance new-variance))
      (assoc acc :current updated))))

(defn generate-bars [target-amt trades]
  (->> (reduce
        add-to-bars
        (bar-base target-amt)
        trades)
       :bars
       drop-last))
