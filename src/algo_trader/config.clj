(ns algo-trader.config
  (:require [clojure.edn :as edn]
            [clojure.string :refer [includes?]]))

(def config (edn/read-string (slurp "resources/config.edn")))

(def instrument
  (cond
    (includes? (:cluster config) "stocks") :stocks
    (includes? (:cluster config) "crypto") :crypto
    :else nil)) ;; polygon also has forex, but didn't implement that for now

(def trade-event
  (condp = instrument
    :stocks :T
    :crypto :XT
    nil))

(def quote-event
  (condp = instrument
    :stocks :Q
    :crypto :XQ
    nil))
