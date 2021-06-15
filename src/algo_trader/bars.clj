(ns algo-trader.bars
  (:require [algo-trader.config :refer [config vol-alpha window-alphas
                                        fc-count fc-window-delta]]
            [algo-trader.utils :refer [pct-rtn ewm-step]]))

(def lag (:lag config))
(def base {:amt 0.0 :v 0.0 :bv 0.0 :sv 0.0})

(defn bar-base [target-amt]
  {:current base
   :bars '()
   :ewms (repeat (:num-windows config) nil)
   :vois '()
   :oirs '()
   :target-amt target-amt})

(defn update-bar [bar {:keys [price size side]}]
  (let [amt (* price size)
        b? (= :buy side)]
    (-> (update bar :o (fnil identity price))
        (assoc :c price)
        (update :amt + amt)
        (update :v + size)
        (update :bv + (if b? size 0.0))
        (update :sv + (if b? 0.0 size)))))

(defn add-to-bars
  [{:keys [current bars ewms vois oirs variance target-amt] :as acc}
   {:keys [price] :as trade}]
  (let [{:keys [o c amt v bv sv] :as updated} (update-bar current trade)]
    (if (>= amt target-amt)
      (let [prev-close (:c (first bars) price)
            new-ewms (mapv #(ewm-step %1 price %2) ewms window-alphas)
            new-ewmacs (mapv - new-ewms (drop fc-window-delta new-ewms))
            voi (- bv sv)
            adj-voi (/ voi 1e2)
            new-vois (take lag (conj vois adj-voi))
            oir (/ voi amt)
            adj-oir (* 5e4 oir)
            new-oirs (take lag (conj oirs adj-oir))
            vwap (/ amt v)
            mpb (/ vwap (/ (+ o c) 2.0))
            adj-mpb (* 1e4 (dec mpb))
            features (vec (concat new-ewmacs new-vois new-oirs [adj-mpb]))
            diff (- price prev-close)
            rtn (pct-rtn prev-close price)
            sq-rtn (Math/pow rtn 2.0)
            new-variance (ewm-step variance sq-rtn vol-alpha)
            new-bar (assoc updated :features features :diff diff)]
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :ewms new-ewms
         :vois new-vois
         :oirs new-oirs
         :variance new-variance))
      (assoc acc :current updated))))

(defn generate-bars [target-amt trades]
  (->> (reduce
        add-to-bars
        (bar-base target-amt)
        trades)
       :bars
       (drop-last (max lag fc-count))))
