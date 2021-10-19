(ns algo-trader.utils
  (:require [algo-trader.config :refer [config vol-scale]]
            [clojure.core.async :refer [chan]]
            [clojure.string :refer [upper-case]]))

;;; math ;;;
(defn pct-rtn [p1 p2]
  (dec (/ p2 p1)))

(defn dot-product [xs ys]
  (apply + (map * xs ys)))

(defn ewm-step [previous observed alpha]
  (if (nil? previous)
    observed
    (+ (* alpha observed) (* (- 1.0 alpha) previous))))

(defn sma-step [previous observed period]
  (if (nil? previous)
    observed
    (/ (+ (* previous (dec period)) observed) period)))

(defn clip [cap x]
  (cond
    (> x cap) cap
    (< x (- cap)) (- cap)
    :else x))

;;; vol ;;;
(defn annual-vol-cash-target [trading-capital]
  (* (:volatility-target config) trading-capital))

(defn bar-vol-cash-target [trading-capital]
  (/ (annual-vol-cash-target trading-capital) vol-scale))

(defn block-value [price block-size]
  (* 0.01 price block-size))                                ;; 1% price move impact

(defn instrument-vol [price block-size vol]
  (* (block-value price block-size) vol 100.0))             ;; 100 because we need vol-pct

(defn vol-scalar [trading-capital price block-size vol]
  (/ (bar-vol-cash-target trading-capital) (instrument-vol price block-size vol)))

;;; seq ;;;
(defn roll-seq
  "adds new value to head, takes first l"
  [xs x l]
  (take l (conj xs x)))

;;; keywords/naming ;;;
(defn uc-kw
  "upper-case keyword"
  [kw-or-str]
  (-> kw-or-str name upper-case keyword))

(defn market-kw [underlying]
  (uc-kw (str (name underlying) "-PERP")))

(defn underlying-kw [market]                                ;; from MARKET
  (let [n (name market)]
    (uc-kw (subs n 0 (- (count n) 5)))))

(defn spot-kw [market]                                      ;; from MARKET
  (let [n (name market)]
    (uc-kw (str (subs n 0 (- (count n) 5)) "/USD"))))

(defn market-from-spot [spot]
  (let [n (name spot)]
    (market-kw (subs n 0 (- (count n) 4)))))

(defn get-target-amts []
  (:target-amts config))

;;; core.async ;;;
(defn generate-channel-map [markets]
  (reduce
    #(assoc %1 (uc-kw %2) (chan 1000))
    {}
    markets))
