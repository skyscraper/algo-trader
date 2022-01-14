(ns algo-trader.bars
  (:require [algo-trader.config :refer [config bar-count vol-scale-daily vol-alpha]]
            [algo-trader.utils :refer [pct-rtn ewm-step sma-step]]))

(def atr-length (:atr-length config))
(def factor (:factor config))
(def base {:v 0.0 :bv 0.0 :sv 0.0})

(def feature-base {:st-features (vec (repeat (count (:bar-widths config)) {}))})

(defn bar-base [target-size]
  {:current       base
   :bars          '()
   :features-data feature-base
   :target-size   target-size})

(defn update-bar [bar {:keys [price size side]}]
  (-> (update bar :o (fnil identity price))
      (update :h (fnil max price) price)
      (update :l (fnil min price) price)
      (assoc :c price)
      (update :v + size)
      (update :bv + (if (= side :buy) size 0.0))
      (update :sv + (if (= side :sell) size 0.0))))

(defn collate
  "assumes first bar is latest"
  [bars]
  (when (seq bars)
    {:h (apply max (map :h bars))
     :l (apply min (map :l bars))
     :c (:c (first bars))}))

(defn supertrend [{:keys [h l c]} prev-close st-features]
  (let [prev-trend-up (:trend-up st-features)
        prev-trend-down (:trend-down st-features)
        true-range (max (- h l)
                        (Math/abs (- h prev-close))
                        (Math/abs (- l prev-close)))
        atr (sma-step (:atr st-features) true-range atr-length)
        mid (/ (+ h l) 2.0)
        up (- mid (* factor atr))
        down (+ mid (* factor atr))
        trend-up (if (and prev-trend-up (> prev-close prev-trend-up))
                   (max up prev-trend-up)
                   up)
        trend-down (if (and prev-trend-down (< prev-close prev-trend-down))
                     (min down prev-trend-down)
                     down)
        trend (if (> c trend-down)
                10.0
                (if (< c trend-up)
                  -10.0
                  (:trend st-features 0.0)))]
    {:atr atr
     :trend-up trend-up
     :trend-down trend-down
     :trend trend}))

(defn add-to-bars
  [{:keys [current bars features-data variance target-size] :as acc}
   {:keys [price side] :as trade}]
  (let [{:keys [o bv sv v] :as updated} (update-bar current trade)]
    (if (>= v target-size)
      (let [last-bar (first bars)
            prev-close (:c last-bar o)
            {:keys [st-features]} features-data
            oi (/ (- bv sv) v)
            oi-diff (- oi (:oi last-bar 0.0))
            collated (mapv #(collate (take % (conj bars updated))) (:bar-widths config))
            prev-closes (mapv (fn [w]
                                (if-let [b (first (drop (dec w) bars))]
                                  (:c b)
                                  (:c (last bars) o)))
                              (:bar-widths config))
            new-st-features (mapv supertrend collated prev-closes st-features)
            sts (mapv :trend new-st-features)
            features (conj sts oi-diff)
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
                           :features features
                           :sigma sigma
                           :sigma-day sigma-day)]
        (assoc
         (update acc :bars conj new-bar)
         :current base
         :features-data {:st-features new-st-features}
         :variance new-variance))
      (assoc acc :current updated))))

(defn generate-bars [target-size trades]
  (->> (reduce
        add-to-bars
        (bar-base target-size)
        trades)
       :bars
       (drop-last bar-count)))
