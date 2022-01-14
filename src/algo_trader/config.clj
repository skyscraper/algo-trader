(ns algo-trader.config
  (:require [clojure.edn :as edn]))

(def config (edn/read-string (slurp "resources/config.edn")))
(def orders-ep "/orders")

(def fc-count (inc (count (:bar-widths config)))) ;; 2 * st + oi
(def bar-count (apply max (:bar-widths config)))

(def default-weights (repeat fc-count (double (/ 1 fc-count))))

(defn get-alpha [x]
  (/ 2.0 (+ 1.0 x)))

(def vol-alpha (get-alpha (:vol-span config)))
(def vol-scale-annual (Math/sqrt (* 365.0 24.0 (/ 60.0 (:est-bar-mins config)))))
(def vol-scale-daily (Math/sqrt (* 24.0 (/ 60.0 (:est-bar-mins config)))))

;; ewm decay for forecast scaling factors
(def scale-alpha
  (get-alpha (:scale-span config)))

(def hardcoded-eq                                           ;; for testing
  (* (count (:target-sizes config)) (:test-trading-capital config)))

;; convenience pre-calc
(def price-slippage
  [(+ 1.0 (/ (:same-slippage-bps config) 1e4))
   (+ 1.0 (/ (:opp-slippage-bps config) 1e4))
   (- 1.0 (/ (:opp-slippage-bps config) 1e4))
   (- 1.0 (/ (:same-slippage-bps config) 1e4))])

(def fee-mults
  [(+ 1.0 (/ (:taker-fee-bps config) 1e4))
   (- 1.0 (/ (:taker-fee-bps config) 1e4))])
