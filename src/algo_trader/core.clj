(ns algo-trader.core
  (:gen-class)
  (:require [algo-trader.api :as api]
            [algo-trader.config :refer [config trade-event quote-event]]
            [algo-trader.statsd :as statsd]
            [cheshire.core :refer [parse-string]]
            [clojure.core.async :refer [<! >! chan go-loop]]
            [clojure.string :refer [join upper-case]]
            [clojure.tools.logging :as log]
            [manifold.deferred :refer [timeout!]]
            [manifold.stream :refer [take!]]))

(def symbols (atom {}))
(def polygon-ws-conn nil)
(def active? (atom true))
(def polygon-timeout (:ws-timeout-ms config))
(def distribution-chan (chan 1000))
(def trade-channels {})
(def quote-channels {})

(defn reset-polygon!
  "reset polygon ws connection"
  []
  (log/info "Connecting to market data stream...")
  (alter-var-root
   #'polygon-ws-conn
   (fn [_] (api/polygon-websocket (:cluster config)))))

(defn uc-kw
  "upper-case keyword"
  [kw-or-str]
  (-> kw-or-str name upper-case keyword))

(defn generate-channel-map [symbols]
  (reduce
   #(assoc %1 (uc-kw %2) (chan 1000))
   {}
   symbols))

(defn start-md-main
  "Main consumer for polygon messages"
  []
  (go-loop []
    (if @active?
      (if-let [raw @(timeout! (take! polygon-ws-conn) polygon-timeout nil)]
        (do
          (>! distribution-chan raw)
          (recur))
        (do
          (log/error "Stopped receiving polygon websocket data! Attempting to reconnect...")
          (.close polygon-ws-conn)
          (reset-polygon!)
          (api/polygon-subscribe polygon-ws-conn @symbols)
          (recur)))
      (log/info "exiting market data processing loop..."))))

(defn start-md-distributor
  "parse and distribute md messgaes"
  []
  (go-loop []
    (let [raw (<! distribution-chan)
          payload (parse-string raw true)]
      (statsd/count :ws-msg 1 nil)
      (doseq [evt payload
              :let [{:keys [ev sym] :as event} (-> (update evt :ev keyword)
                                                   (update :sym keyword))]]
        (condp = ev
          trade-event (when-let [c (sym trade-channels)] (>! c event))
          quote-event (when-let [c (sym quote-channels)] (>! c event))
          :status (log/info (:message event))
          (log/warn (str "unhandled polygon event: " event))))
      (recur))))

(defn start-md-handlers
  "Starts a go-loop and applies a fn to every event received on a given channel.
   Event types should be homogenous."
  [f channels]
  (doseq [c (vals channels)]
    (go-loop []
      (f (<! c))
      (recur))))

(defn handle-quote
  "handle quote - currently just calculates delay from current time and sends to statsd."
  [{:keys [t sym]}]
  (let [l (list sym)]
    (statsd/count :quote 1 l)
    (let [ts (System/currentTimeMillis)
          md-delay (- ts t)]
      (statsd/distribution :quote-delay md-delay nil)
      ;; TODO: your logic here!
      nil)))

(defn handle-trade
  "handle trade - currently just calculates delay from current time and sends to statsd."
  [{:keys [t sym]}]
  (let [l (list sym)]
    (statsd/count :trade 1 l)
    (let [ts (System/currentTimeMillis)
          md-delay (- ts t)]
      (statsd/distribution :trade-delay md-delay nil)
      ;; TODO: your logic here!
      nil)))

(defn run
  []
  (log/info "Connecting to statsd...")
  (statsd/reset-statsd!)
  (log/info "Connecting to polygon...")
  (reset-polygon!)
  (reset! symbols (map uc-kw (:symbols config)))
  (log/info "Today we will be trading:" (join ", " (map name @symbols)))
  (log/info "Starting md handlers...")
  (start-md-handlers handle-trade trade-channels)
  (start-md-handlers handle-quote quote-channels)
  (log/info "Subscribing to market data...")
  (api/polygon-subscribe polygon-ws-conn @symbols))

(defn -main
  [& args]
  (run))
