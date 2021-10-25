(ns algo-trader.bars
  (:require [algo-trader.config :refer [config vol-alpha windows st-windows
                                        window-alphas fc-count fc-window-delta]]
            [algo-trader.utils :refer [pct-rtn ewm-step sma-step roll-seq]]))

(def included? (set (:included-features config)))
(def range-max (:scale-cap config))

(def base {:amt 0.0 :v 0.0 :twobv 0.0 :lasts {}})

(def feature-base
  {:ewms        (repeat (:num-windows config) nil)
   :gains       '()
   :losses      '()
   :st-features (repeat fc-count {})})

(defn bar-base [target-amt]
  {:current       base
   :bars          '()
   :features-data feature-base
   :target-amt    target-amt})

(defn update-bar [{:keys [lasts] :as bar} {:keys [price size side source]}]
  (let [last-price (source lasts)
        s (if last-price
            (cond
              (> price last-price) :buy
              (< price last-price) :sell
              :else side)
            side)]
    (-> (update bar :o (fnil identity price))
        (update :h (fnil max price) price)
        (update :l (fnil min price) price)
        (assoc :c price)
        (update :amt + (* price size))
        (update :v + size)
        (update :twobv + (* size (if (= :buy s) 2.0 0.0)))
        (assoc :last-side s) ;; todo: maybe take avg
        (assoc-in [:lasts source] price))))

(defn rsi [gains losses w]
  (let [ls (/ (apply + (take w losses)) w)]
    (if (zero? ls)
      range-max
      (- range-max
         (/ range-max
            (+ 1.0
               (/ (/ (apply + (take w gains)) w)
                  ls)))))))

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
        atr (sma-step (:atr st-features) true-range 3.0)
        mid (/ (+ h l) 2.0)
        up (- mid atr)
        down (+ mid atr)
        trend-up (if (and prev-trend-up (> prev-close prev-trend-up))
                   (max up prev-trend-up)
                   up)
        trend-down (if (and prev-trend-down (< prev-close prev-trend-down))
                     (min down prev-trend-down)
                     down)
        x (max (- c trend-down) 0.0)
        y (min (- c trend-up) 0.0)
        trend (if (and (zero? x) (zero? y))
                (:trend st-features 0.0)
                (if (> x (Math/abs y)) x y))]
    {:atr        atr
     :trend-up   trend-up
     :trend-down trend-down
     :trend      trend}))

(defn add-to-bars
  [{:keys [current bars features-data variance target-amt] :as acc}
   {:keys [price] :as trade}]
  (let [{:keys [o amt] :as updated} (update-bar current trade)]
    (if (>= amt target-amt)
      (let [prev-close (:c (first bars) o)
            {:keys [ewms gains losses st-features]} features-data
            ;; ewmacs
            new-ewms (mapv #(ewm-step %1 price %2) ewms window-alphas)
            new-ewmacs (mapv - new-ewms (drop fc-window-delta new-ewms))
            ;; rsix
            diff (- price prev-close)
            abs-diff (Math/abs diff)
            new-gains (roll-seq gains (if (pos? diff) diff 0.0) (last windows))
            new-losses (roll-seq losses (if (pos? diff) 0.0 abs-diff) (last windows))
            new-rsis (mapv #(rsi new-gains new-losses %) windows)
            new-rsix (mapv - new-rsis (drop fc-window-delta new-rsis))
            ;; supertrend
            collated (mapv #(collate (take % (conj bars updated))) st-windows)
            prev-closes (mapv (fn [w]
                                (if-let [b (first (drop w bars))]
                                  (:c b)
                                  (:c (last bars) o)))
                              st-windows)
            new-st-features (mapv supertrend collated prev-closes st-features)
            sts (mapv :trend new-st-features)
            features (vec (concat (when (:ewmacs included?) new-ewmacs)
                                  (when (:rsix included?) new-rsix)
                                  (when (:supertrend included?) sts)))
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
          :features-data {:ewms        new-ewms
                          :gains       new-gains
                          :losses      new-losses
                          :st-features new-st-features}
          :variance new-variance))
      (assoc acc :current updated))))

(defn generate-bars [target-amt trades]
  (->> (reduce
         add-to-bars
         (bar-base target-amt)
         trades)
       :bars
       (drop-last (:bar-count config))))
