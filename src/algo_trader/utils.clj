(ns algo-trader.utils
  (:require [algo-trader.config :refer [config vol-scale-annual]]
            [clojure.string :refer [upper-case]]))

;;; math ;;;
(defn pct-rtn [p1 p2]
  (- (/ p2 p1) 1.0))

(defn dot-product [xs ys]
  (apply + (map * xs ys)))

(defn ewm-step [previous observed alpha]
  (if (nil? previous)
    observed
    (+ (* alpha observed) (* (- 1.0 alpha) previous))))

(defn clip [cap x]
  (cond
    (> x cap) cap
    (< x (- cap)) (- cap)
    :else x))

;;; vol ;;;
(defn annual-vol-cash-target [trading-capital]
  (* (:volatility-target config) trading-capital))

(defn bar-vol-cash-target [trading-capital]
  (/ (annual-vol-cash-target trading-capital) vol-scale-annual))

(defn block-value [price block-size]
  (* 0.01 price block-size))                                ;; 1% price move impact

(defn instrument-vol [price block-size sigma]
  (* (block-value price block-size) sigma 100.0))           ;; 100 because we need vol-pct

(defn vol-scalar [trading-capital price block-size sigma]
  (/ (bar-vol-cash-target trading-capital) (instrument-vol price block-size sigma)))

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

(defn get-target-sizes []
  (:target-sizes config))

;;; core.async ;;;
(defn generate-channel-map
  "creates a map of channels with key as the uppercase-keywordized version of markets
  and value as calling chan-fn"
  [markets chan-fn]
  (reduce
    #(assoc %1 (uc-kw %2) (chan-fn))
    {}
    markets))
