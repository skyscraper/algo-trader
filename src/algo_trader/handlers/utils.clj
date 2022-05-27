(ns algo-trader.handlers.utils
  (:require [aleph.http :as http]
            [algo-trader.statsd :as statsd]
            [byte-streams :as bs]
            [clojure.core.async :refer [<! >! go go-loop timeout]]
            [clojure.string :refer [lower-case]]
            [java-time :refer [instant zoned-date-time]]
            [jsonista.core :as json]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [taoensso.timbre :as log]))

;; constants
(def coin-str "coin")
(def side-str "side")
(def inv-true "inverse:true")
(def inv-false "inverse:false")

;; exchange specific map
(defn info-map [existing rename-fn trade-channels]
  (reduce-kv
   (fn [acc sym ch]
     (assoc
      acc
      (rename-fn sym)
      {:channel ch
       :coin-tag (str coin-str sym)
       :price-gauge (keyword (str (lower-case (name sym)) "-price"))}))
   existing
   trade-channels))

;; time
(defn epoch [dt-str]
  (.toEpochMilli (instant (zoned-date-time dt-str))))

;; ws
(defn check-failure [x]
  (condp = x
    :source-failure (throw (Exception. "try-take failure"))
    :timeout-failure (throw (Exception. "delivery timeout"))
    x))

;; ping
(defn ping-loop [conn interval payload]
  (go-loop []
    (<! (timeout interval))
    (when @(s/put! conn payload)
      (recur))))

(defn connect! [exch url props ws-timeout handle-fn payloads
                {:keys [interval payload] :as ping-params}]
  (log/info "connecting to" (name exch) "...")
  (->
   (d/let-flow [conn (http/websocket-client url (merge {:epoll? true} props))
                handle (fn [raw] (handle-fn raw conn))]
     (when ping-params
       (ping-loop conn interval payload))
     (log/info "subscribing to feeds for" (name exch) "...")
     (doseq [p payloads]
       (s/put! conn (json/write-value-as-string p)))
     (d/loop []
       (d/chain
        (s/try-take! conn :source-failure ws-timeout :timeout-failure)
        check-failure
        handle
        (fn [_] (d/recur)))))
   (d/catch
       (fn [e]
         (log/error (name exch) "ws problem:" e)
         (connect! exch url props ws-timeout handle-fn payloads ping-params)))))

;; contract sizing
(defn get-ct-sizes [exch url data-kw r-fn]
  (log/info (format "fetching ct sizes for %s..." (name exch)))
  (let [data (-> url
                 http/get
                 deref
                 :body
                 bs/to-string
                 (json/read-value json/keyword-keys-object-mapper)
                 data-kw)]
    (reduce r-fn {} data)))

;; trade stats
(defn trade-stats [{:keys [price size time side]} tags {:keys [coin-tag price-gauge]}]
  (let [trade-delay (- (System/currentTimeMillis) time)
        amt (* price size)]
    (statsd/distribution :trade-delay trade-delay tags)
    (statsd/count :trade 1 tags)
    (statsd/count :notional amt (conj tags coin-tag (str side-str side)))
    (statsd/gauge price-gauge price tags)))

;; process incoming market data and report stats
(defn process-single [trade tags {:keys [channel] :as meta-info}]
  (go
    (when (>! channel trade)
      (trade-stats trade tags meta-info))))

(defn process [trades tags {:keys [channel] :as meta-info}]
  (go-loop [vs (seq trades)]
    (when (and vs (>! channel (first vs)))
      (trade-stats (first vs) tags meta-info)
      (recur (next vs)))))
