(ns algo-trader.handlers.huobi-inv
  (:require [algo-trader.statsd :as statsd]
            [algo-trader.handlers
             [huobi :as huobi]
             [utils :refer [connect! info-map inv-true process]]]
            [byte-streams :as bs]
            [clojure.java.io :refer [reader]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [taoensso.timbre :as log])
  (:import (java.util.zip GZIPInputStream)))

(def url "wss://api.hbdm.vn/swap-ws")
(def exch :huobi-inv)
(def tags [(str "exch" huobi/exch) inv-true])
(def ws-props {:max-frame-payload 262144})
(def ws-timeout 20000)
(def info {})

(defn handle [raw conn]
  (with-open [rdr (reader (GZIPInputStream. (bs/to-input-stream raw)))]
    (let [{:keys [ch subbed status ping tick] :as payload}
          (json/read-value rdr json/keyword-keys-object-mapper)]
      (statsd/count :ws-msg 1 tags)
      (cond
        (some? ch) (process (map #(huobi/normalize % exch) (:data tick)) tags ((keyword ch) info))
        (some? subbed) (log/info subbed "subscription status:" status)
        (some? ping) (s/put! conn (json/write-value-as-string {:pong ping}))
        :else (log/warn "unhandled huobi-inv message: " payload)))))

(defn rename [k]
  (keyword (format huobi/base (str (name k) "-USD"))))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect! exch url ws-props ws-timeout handle (huobi/subscribe-msgs (keys info)) nil))
