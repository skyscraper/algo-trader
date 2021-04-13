(ns algo-trader.oms
  (:require [algo-trader.config :refer [config]]))

(def equity (atom 0.0))
(def max-pos-notional (atom 0.0))
(def min-pos-notional (atom 0.0))

(defn initialize-equity [eq]
  (reset! equity eq))

(defn get-max-notional [equity n-pairs]
  (let [eq (/ (* 0.95 equity) n-pairs)] ;; divide 95% of equity evenly
    (min (:max-pos-notional config) eq))) ;; take min of split and hard limit

(defn get-min-notional [max-notional]
  (* (:min-pos-notional-percent config) max-notional))

(defn determine-notionals [pairs]
  (let [max-notional (get-max-notional @equity (count pairs))]
    (reset! min-pos-notional (get-min-notional max-notional))
    (reset! max-pos-notional max-notional))) ;; last to return max

;; just a few stub methods so far... todo: order handling!
