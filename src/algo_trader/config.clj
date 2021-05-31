(ns algo-trader.config
  (:require [clojure.edn :as edn]))

(def config (edn/read-string (slurp "resources/config.edn")))
(def jump 2)
(def fc-window-delta jump)
(def fc-count (- (:num-windows config) fc-window-delta))

;; windows to calculate
(def windows
  (->> (:window-start config)
       (iterate #(* % jump))
       (take (:num-windows config))
       vec))

(defn get-alpha [x]
  (/ 2.0 (inc x)))

(def vol-alpha (get-alpha (:vol-span config)))
(def vol-scale (Math/sqrt (* 365.0 24.0 (/ 60.0 (:est-bar-mins config)))))

;; ewm decay for windows
(def window-alphas
  (mapv get-alpha windows))

;; convenience for mf
(def fast-alphas
  (vec (take fc-count window-alphas)))

;; ewm decay for forecast scaling factors
(def scale-alpha
  (get-alpha (:scale-span config)))

(def hardcoded-eq ;; for testing
  (let [c (count (:target-amts config))
        n-markets (if (zero? c) (:num-markets config) c)]
    (* n-markets (:test-market-notional config))))
