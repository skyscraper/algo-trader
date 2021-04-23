(ns algo-trader.config
  (:require [clojure.edn :as edn]))

(def config (edn/read-string (slurp "resources/config.edn")))

;; pre-calculating some values for repeated volatility calcs...
(def vol-alpha (/ 2.0 (inc (:vol-span config))))
(def diff-alpha (- 1.0 vol-alpha))
(def vol-weights (take (:vol-span config) (iterate #(* % diff-alpha) 1.0)))
(def sum-weights (apply + vol-weights))
(def jump 2)
(def fc-window-delta jump)
(def fc-count (- (:num-windows config) fc-window-delta))
(def minutes-in-day (* 24 60))

(def default-weights (repeat fc-count (double (/ 1 fc-count))))

;; windows to calculate
(def windows
  (->> (:window-start config)
       (iterate #(* % jump))
       (take (:num-windows config))
       vec))

(defn get-alpha [x]
  (/ 2.0 (inc x)))

;; ewm decay for windows
(def window-alphas
  (mapv get-alpha windows))

;; ewm decay for forecast scaling factors
(def scale-alpha
  (get-alpha (:scale-span config)))

(def hardcoded-eq
  (* (:num-markets config) (/ (:max-pos-notional config) 3)))
