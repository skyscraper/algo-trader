(ns algo-trader.config
  (:require [clojure.edn :as edn]))

(def config
  (let [c (edn/read-string (slurp "resources/config.edn"))]
    (assoc c :markets (vec (keys (:target-amts c))))))

;; pre-calculating some values for repeated volatility calcs...
(def vol-alpha (/ 2.0 (inc (:vol-span config))))
(def diff-alpha (- 1.0 vol-alpha))
(def vol-weights (take (:vol-span config) (iterate #(* % diff-alpha) 1.0)))
(def sum-weights (apply + vol-weights))

;; windows to calculate
(def windows
  (let [s2 (Math/sqrt 2)]
    (->> (:window-start config)
         (iterate #(* % s2))
         (take (:num-windows config))
         vec)))

;; ewm decay for windows
(def window-alphas
  (mapv
   (fn [w] (/ 2.0 (inc w)))
   windows))

;; ewm decay for scale factors
(def scale-alphas
  (mapv
   (fn [w] (/ 2.0 (* w (:scale-mult config))))
   windows))

(def fc-window-delta 4) ;; this is because we are jumping by sqrt(2)
(def fc-count (- (:num-windows config) fc-window-delta))
