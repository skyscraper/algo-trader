(ns algo-trader.handlers.ftx
  (:require [algo-trader.statsd :as statsd]
            [algo-trader.handlers.utils :refer [connect! epoch info-map inv-false
                                                process]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [taoensso.timbre :as log]))

(def url "wss://ftx.com/ws/")
(def exch :ftx)
(def tags [(str "exch" exch) inv-false])
(def ws-props {:max-frame-payload 131072})
(def ws-timeout 20000)
(def info {})
(def ping-params {:interval 15000
                  :payload (json/write-value-as-string {:op :ping})})
(def sub-base {:op :subscribe :channel :trades})

(defn normalize [x]
  (-> (update x :time epoch)
      (update :side keyword)
      (assoc :source exch)))

(defn handle [raw conn]
  (let [{:keys [channel market type code msg data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (condp = (keyword type)
      :update (process (map normalize data) tags ((keyword market) info))
      :partial (log/warn (format "received partial event: %s" payload))
      :info (do (log/info (format "ftx info: %s %s" code msg))
                (when (= code 20001)
                  (log/info (name exch) "server requested us to reconnect...")
                  (s/close! conn)))
      :subscribed (log/info (format "subscribed to %s %s" market channel))
      :unsubscribed (log/info (format "unsubscribed from %s %s" market channel))
      :error (log/error (format "ftx error: %s %s" code msg))
      :pong (log/debug "pong")
      (log/warn (str "unhandled ftx event: " payload)))))

(defn rename [k]
  (keyword (str (name k) "-PERP")))

(defn subscribe-msgs []
  (map #(assoc sub-base :market %) (keys info)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect! exch url ws-props ws-timeout handle (subscribe-msgs) ping-params))
