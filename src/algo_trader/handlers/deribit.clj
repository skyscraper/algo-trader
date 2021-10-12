(ns algo-trader.handlers.deribit
  (:require [algo-trader.statsd :as statsd]
            [algo-trader.handlers.utils :refer [connect! info-map inv-false process]]
            [clojure.string :refer [includes? join]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [taoensso.timbre :as log]))

(def url "wss://www.deribit.com/ws/api/v2")
(def api-test "public/test")
(def api-hb "public/set_heartbeat")
(def api-sub "public/subscribe")
(def exch :deribit)
(def tags [(str "exch" exch) inv-false])
(def ws-props {:max-frame-payload 131072})
(def ws-timeout 60000)
(def info {})
(def heartbeat-interval-sec 30)
(def base "trades.%s-PERPETUAL.raw")

(defn msg-base []
  {:jsonrpc "2.0"
   :id (System/currentTimeMillis)})

(defn normalize [{:keys [price amount direction timestamp liquidation]}]
  {:price price
   :size (double (/ amount price))
   :side (keyword direction)
   :time timestamp
   :liquidation (and (some? liquidation) (includes? liquidation "T"))
   :source exch})

(defn handle [raw conn]
  (let [{:keys [result method params] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (if method
      (condp = (keyword method)
        :subscription
        (let [{:keys [data channel]} params]
          (process (map normalize data) tags ((keyword channel) info)))
        :heartbeat
        (s/put! conn (json/write-value-as-string (assoc (msg-base)
                                                        :method api-test
                                                        :params {})))
        (log/warn "unhandled deribit method" payload))
      (if result
        (when (vector? result)
          (log/info "subscribed to:" (join "," result)))
        (log/warn "unhandled result:" payload)))))

(defn rename [k]
  (keyword (format base (name k))))

(defn subscribe-msgs []
  [(assoc (msg-base)
          :method api-hb
          :params {:interval heartbeat-interval-sec})
   (assoc (msg-base)
          :method api-sub
          :params {:channels (keys info)})])

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect! exch url ws-props ws-timeout handle (subscribe-msgs) nil))
