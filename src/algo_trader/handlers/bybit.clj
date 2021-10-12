(ns algo-trader.handlers.bybit
  (:require [algo-trader.statsd :as statsd]
            [algo-trader.handlers.utils :refer [connect! info-map inv-false process]]
            [clojure.string :refer [join lower-case]]
            [jsonista.core :as json]
            [taoensso.timbre :as log]))

(def url "wss://stream.bybit.com/realtime_public")
(def exch :bybit)
(def tags [(str "exch" exch) inv-false])
(def ws-props {:max-frame-payload 262144})
(def ws-timeout 30000)
(def info {})
(def ping-params {:interval 30000
                  :payload (json/write-value-as-string {:op :ping})})
(def sub-base {:op :subscribe})

(defn normalize [{:keys [price size trade_time_ms side]}]
  {:price (Double/parseDouble price)
   :size (double size)
   :side (keyword (lower-case side))
   :time (Long/parseLong trade_time_ms)
   :source exch})

(defn handle [raw _]
  (let [{:keys [request success ret_msg topic data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (cond
      (some? request) (if success
                        (condp = (keyword (:op request))
                          :subscribe (log/info "subscribed to" (join "," (:args request)))
                          :ping (log/debug "pong")
                          (log/info request))
                        (log/warn ret_msg))
      (some? topic) (process (map normalize data) tags ((keyword topic) info))
      :else (log/warn (str "unhandled bybit event:" payload)))))

(defn rename [k]
  (keyword (format "trade.%sUSDT" (name k))))

(defn subscribe-msgs [args]
  [(assoc sub-base :args args)])

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect! exch url ws-props ws-timeout handle (subscribe-msgs (keys info)) ping-params))
