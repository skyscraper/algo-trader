(ns algo-trader.handlers.ftx-us
  (:require [algo-trader.api :refer [hmac ws-sig]]
            [algo-trader.config :refer [config]]
            [algo-trader.handlers
             [ftx :as ftx]
             [utils :refer [connect! info-map inv-false]]]
            [clojure.core.async :refer [put!]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [taoensso.timbre :as log]))

(def url "wss://ftx.us/ws/")
(def exch :ftx-us)
(def tags [(str "exch" ftx/exch) inv-false])
(def info {})
(def fills {:op :subscribe :channel :fills})
(def orders {:op :subscribe :channel :orders})

(defn process-orders-fills [market msg-type data]
  (put! ((keyword market) info) (assoc data :msg-type msg-type)))

(defn handle [raw conn]
  (let [{:keys [channel market type code msg data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (condp = (keyword type)
      :update (process-orders-fills market (keyword channel) data)
      :partial (log/warn (format "received partial event: %s" payload))
      :info (do (log/info (format "ftx us info: %s %s" code msg))
                (when (= code 20001)
                  (log/info (name exch) "server requested us to reconnect...")
                  (s/close! conn)))
      :subscribed (log/info (format "subscribed to %s" channel))
      :unsubscribed (log/info (format "unsubscribed from %s" channel))
      :error (log/error (format "ftx us error: %s %s" code msg))
      :pong (log/debug "pong")
      (log/warn (str "unhandled ftx us event: " payload)))))

(defn rename [k]
  (keyword (str (name k) "/USD")))

(defn auth [subaccount]
  (let [now (System/currentTimeMillis)
        args {:key  (:ftx-us-api-key config)
              :sign (hmac (ws-sig now))
              :time now}]
    {:op :login
     :args (if subaccount
             (assoc args :subaccount subaccount)
             args)}))

(defn init [oms-channels]
  (alter-var-root #'info info-map rename oms-channels)
  (connect! exch url ftx/ws-props ftx/ws-timeout handle (conj [(auth (:subaccount config))] fills orders)
            ftx/ping-params))
