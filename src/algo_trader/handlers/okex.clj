(ns algo-trader.handlers.okex
  (:require [algo-trader.statsd :as statsd]
            [algo-trader.handlers.utils :refer [connect! get-ct-sizes info-map inv-false
                                                inv-true process]]
            [jsonista.core :as json]
            [taoensso.timbre :as log]))

(def api-url "https://aws.okex.com/api/v5/public")
(def inst-ep "/instruments?instType=SWAP")
(def url "wss://wsaws.okex.com:8443/ws/v5/public")
(def exch :okex)
(def tags [(str "exch" exch)])
(def ws-props {:max-frame-payload 131072
               :compression? true
               :heartbeats {:send-after-idle 3e4
                            :payload "ping"
                            :timeout 3e4}})
(def ws-timeout 20000)
(def info {})
(def sub-base {:channel :trades})
(def ct-size (atom {}))

(defn normalize [{:keys [px sz side ts]} cts ct-type]
  (let [price (Double/parseDouble px)]
    {:price price
     :size (/ (* (Double/parseDouble sz) cts) (if (= :linear ct-type) 1.0 price))
     :side (keyword side)
     :time (Long/parseLong ts)
     :source exch}))

(defn handle [raw _]
  (let [{:keys [event arg data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)
        {:keys [channel instId]} arg]
    (statsd/count :ws-msg 1 tags)
    (if event
      (log/info event channel instId)
      (condp = (keyword channel)
        :trades (when instId
                  (let [kw-inst (keyword instId)
                        [cts ct-type] (kw-inst @ct-size)
                        trades (map #(normalize % cts ct-type) data)
                        extra-tag (if (= :linear ct-type) inv-false inv-true)]
                    (process trades (conj tags extra-tag) (kw-inst info))))
        (log/warn (str "unhandled okex event: " payload))))))

(defn ct-r-fn [acc {:keys [instId ctVal ctType]}]
  (let [k (keyword instId)]
    (if (k info)
      (assoc acc k [(Double/parseDouble ctVal) (keyword ctType)])
      acc)))

(defn rename [k]
  (keyword (str (name k) "-USDT-SWAP")))

(defn rename-inverse [k]
  (keyword (str (name k) "-USD-SWAP")))

(defn subscribe-msgs []
  [{:op :subscribe
    :args (mapv #(assoc sub-base :instId %) (keys info))}])

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (alter-var-root #'info info-map rename-inverse trade-channels)
  (reset! ct-size (get-ct-sizes exch (str api-url inst-ep) :data ct-r-fn))
  (connect! exch url ws-props ws-timeout handle (subscribe-msgs) nil))
