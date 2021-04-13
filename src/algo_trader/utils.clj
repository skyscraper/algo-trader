(ns algo-trader.utils
  (:require [algo-trader.config :refer [config vol-weights sum-weights]]
            [java-time :refer [instant zoned-date-time]]))

(defn log-rtn [p1 p2]
  (Math/log (/ p2 p1)))

(defn dot-product [xs ys]
  (apply + (map * xs ys)))

(defn get-sum-weights [c]
  (if (>= c (:vol-span config)) sum-weights (apply + (take c vol-weights))))

(defn ewm
  "adapted from the pandas implementation, with adjust=True
  IMPORTANT: the weights are ordered from most recent to least, xs must be as well"
  [xs]
  (/ (dot-product vol-weights xs) (get-sum-weights (count xs))))

(defn ewm-both
  "similar to above, xs must be ordered with most recent first"
  [xs]
  (let [c (count xs)
        sw (get-sum-weights c)
        mu (/ (dot-product vol-weights xs) sw)
        sigma (Math/sqrt
               (/ (apply + (map #(* %1 (Math/pow (- %2 mu) 2)) vol-weights xs))
                  sw))]
    [mu sigma]))

(defn ewm-vol [xs]
  (last (ewm-both xs)))

(defn ewm-step [previous observed alpha]
  (if (nil? previous)
    observed
    (+ (* alpha observed) (* (- 1.0 alpha) previous))))

(defn roll-seq
  "adds new value to head, takes first l"
  [xs x l]
  (take l (conj xs x)))

(defn epoch [dt-str]
  (.toEpochMilli (instant (zoned-date-time dt-str))))
