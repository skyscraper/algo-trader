(ns algo-trader.bars
  (:require [algo-trader.config :refer [config vol-alpha windows window-alphas
                                       fc-window-delta]]
            [algo-trader.utils :refer [pct-rtn ewm-step roll-seq]]))

(def range-max (:scale-cap config))

(def base {:amt 0.0 :v 0.0})

(defn bar-base [target-amt]
  {:current base
   :bars '()
   :ewms (repeat (:num-windows config) nil)
   :gains '()
   :losses '()
   :target-amt target-amt})

(defn update-bar [bar {:keys [price size side]}]
  (-> (update bar :o (fnil identity price))
      (assoc :c price)
      (update :amt + (* price size))
      (update :v + size)
      (assoc :last-side side)))

(defn rsi [gains losses w]
  (let [ls (/ (apply + (take w losses)) w)]
    (if (zero? ls)
      range-max
      (- range-max
         (/ range-max
            (+ 1.0
               (/ (/ (apply + (take w gains)) w)
                  ls)))))))

(defn add-to-bars
  [{:keys [current bars ewms gains losses variance target-amt] :as acc}
   {:keys [price] :as trade}]
  (let [{:keys [o c amt v] :as updated} (update-bar current trade)]
    (if (>= amt target-amt)
      (let [prev-close (:c (first bars) o)
            new-ewms (mapv #(ewm-step %1 price %2) ewms window-alphas)
            new-ewmacs (mapv - new-ewms (drop fc-window-delta new-ewms))
            diff (- price prev-close)
            abs-diff (Math/abs diff)
            vwap (/ amt v)
            new-gains (roll-seq gains (if (pos? diff) diff 0.0) (last windows))
            new-losses (roll-seq losses (if (pos? diff) 0.0 abs-diff) (last windows))
            new-rsis (mapv #(rsi new-gains new-losses %) windows)
            new-rsix (mapv - new-rsis (drop fc-window-delta new-rsis))
            mpb (/ vwap (/ (+ o c) 2.0))
            adj-mpb (* 6e3 (dec mpb))
            features (vec (concat new-ewmacs new-rsix [adj-mpb]))
            rtn (pct-rtn prev-close price)
            sq-rtn (Math/pow rtn 2.0)
            new-variance (ewm-step variance sq-rtn vol-alpha)
            sigma (Math/sqrt new-variance)
            new-bar (assoc updated
                           :features features
                           :diff diff
                           :sigma sigma)]
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :ewms new-ewms
         :pmfs new-gains
         :nmfs new-losses
         :variance new-variance))
      (assoc acc :current updated))))

(defn generate-bars [target-amt trades]
  (->> (reduce
        add-to-bars
        (bar-base target-amt)
        trades)
       :bars
       (drop-last (:bar-count config))))
