(ns algo-trader.handlers.huobi
  (:require [algo-trader.statsd :as statsd]
            [algo-trader.handlers.utils :refer [connect! info-map inv-false process]]
            [byte-streams :as bs]
            [clojure.java.io :refer [reader]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [taoensso.timbre :as log])
  (:import (java.util.zip GZIPInputStream)))

(def url "wss://api.hbdm.vn/linear-swap-ws")
(def exch :huobi)
(def tags [(str "exch" exch) inv-false])
(def ws-props {:max-frame-payload 262144})
(def ws-timeout 20000)
(def info {})
(def base "market.%s.trade.detail")

(defn normalize [{:keys [price quantity direction ts]}]
  {:price (double price)
   :size (double quantity)
   :side (keyword direction)
   :time ts
   :source exch})

(defn handle [raw conn]
  (with-open [rdr (reader (GZIPInputStream. (bs/to-input-stream raw)))]
    (let [{:keys [ch subbed status ping tick] :as payload}
          (json/read-value rdr json/keyword-keys-object-mapper)]
      (statsd/count :ws-msg 1 tags)
      (cond
        (some? ch) (process (map normalize (:data tick)) tags ((keyword ch) info))
        (some? subbed) (log/info subbed "subscription status:" status)
        (some? ping) (s/put! conn (json/write-value-as-string {:pong ping}))
        :else (log/warn "unhandled huobi message: " payload)))))

(defn rename [k]
  (keyword (format base (str (name k) "-USDT"))))

(defn subscribe-msgs [xs]
  (map #(hash-map :sub %) xs))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect! exch url ws-props ws-timeout handle (subscribe-msgs (keys info)) nil))
