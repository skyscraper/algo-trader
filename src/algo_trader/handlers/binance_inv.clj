(ns algo-trader.handlers.binance-inv
  (:require [algo-trader.statsd :as statsd]
            [algo-trader.handlers
             [binance :as binance]
             [utils :refer [connect! get-ct-sizes info-map inv-true
                            process-single]]]
            [jsonista.core :as json]
            [taoensso.timbre :as log]))

(def api-url "https://dapi.binance.com")
(def ex-info-ep "/dapi/v1/exchangeInfo")
(def url "wss://dstream.binance.com")
(def exch :binance-inv)
(def tags [(str "exch" binance/exch) inv-true])
(def ws-timeout 20000)
(def info {})
(def ct-size (atom {}))

(defn handle [raw _]
  (let [{:keys [s p q T m] :as payload} (:data (json/read-value raw json/keyword-keys-object-mapper))]
    (statsd/count :ws-msg 1 tags)
    (if s
      (let [kw-sym (keyword s)
            cts (kw-sym @ct-size)
            {:keys [price] :as norm} (binance/normalize p q m T exch)
            trade (update norm :size #(/ (* % cts) price))]
        (process-single trade tags (kw-sym info)))
      (log/warn "unhandled binance-inv message:" payload))))

(defn ct-r-fn [acc {:keys [symbol contractSize contractType]}]
  (let [k (keyword symbol)]
    (if (and (k info) (= :PERPETUAL (keyword contractType)))
      (assoc acc k (double contractSize))
      acc)))

(defn rename [k]
  (keyword (str (name k) "USD_PERP")))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (reset! ct-size (get-ct-sizes exch (str api-url ex-info-ep) :symbols ct-r-fn))
  (connect! exch (binance/full-url url (keys info)) nil ws-timeout handle nil nil))
