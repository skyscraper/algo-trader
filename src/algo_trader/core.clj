(ns algo-trader.core
  (:gen-class)
  (:require [algo-trader.api :as api]
            [algo-trader.config :refer [config]]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.statsd :as statsd]
            [cheshire.core :refer [parse-string]]
            [clojure.core.async :refer [<! >! chan go-loop]]
            [clojure.string :refer [join upper-case]]
            [clojure.tools.logging :as log]
            [manifold.deferred :refer [timeout!]]
            [manifold.stream :refer [take!]]))

(def pairs (atom {}))
(def ftx-ws-conn nil)
(def active? (atom true))
(def ws-timeout (:ws-timeout-ms config))
(def distribution-chan (chan 1000))
(def trade-channels {})
(def quote-channels {})
(def signal (java.util.concurrent.CountDownLatch. 1))

(defn reset-ftx!
  "reset ftx ws connection"
  []
  (log/info "Connecting to market data stream...")
  (alter-var-root
   #'ftx-ws-conn
   (fn [_] (api/ftx-websocket (:ftx-ws config)))))

(defn restart-ftx! []
  (log/info "restarting ftx")
  (.close ftx-ws-conn)
  (reset-ftx!)
  (api/ftx-subscribe-all ftx-ws-conn :trades @pairs))

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
  "Main consumer for ftx messages"
  []
  (go-loop []
    (if @active?
      (if-let [raw @(timeout! (take! ftx-ws-conn) ws-timeout nil)]
        (do
          (>! distribution-chan raw)
          (recur))
        (do
          (log/error "Stopped receiving ftx websocket data! Attempting to reconnect...")
          (restart-ftx!)
          (recur)))
      (log/info "exiting market data processing loop..."))))

(defn start-md-distributor
  "parse and distribute md messgaes"
  []
  (go-loop []
    (let [raw (<! distribution-chan)
          {:keys [channel market type code msg] :as payload}
          (update (parse-string raw true) :type keyword)]
      (statsd/count :ws-msg 1 nil)
      (condp = type
        :update (let [event (update payload :market keyword)
                      c ((:market event) trade-channels)]
                  (when c
                    (>! c event)))
        :partial (log/warn (format "received partial event: %s" payload)) ;; not currently implemented
        :info (do (log/info (format "ftx info: %s %s" code msg))
                  (when (= code 20001)
                    (restart-ftx!)))
        :subscribed (log/info (format "subscribed to %s %s" market channel))
        :unsubscribed (log/info (format "unsubscribed from %s %s" market channel))
        :error (log/error (format "ftx error: %s %s" code msg))
        :pong (log/debug "pong") ;; todo: debug 
        (log/warn (str "unhandled ftx event: " payload)))
      (recur))))

(defn start-md-handlers
  "Starts a go-loop and applies a fn to every event received on a given channel.
   Event types should be homogenous."
  [f channels]
  (doseq [c (vals channels)]
    (go-loop []
      (f (<! c))
      (recur))))

(defn run
  []
  (log/info "Connecting to statsd...")
  (statsd/reset-statsd!)
  (log/info "Connecting to ftx...")
  (reset-ftx!)
  (reset! pairs (map uc-kw (:pairs config)))
  (log/info "Today we will be trading:" (join ", " (map name @pairs)))
  (alter-var-root #'trade-channels merge (generate-channel-map @pairs))
  (log/info "Initializing positions...")
  (model/initialize (:target-amts config))
  (oms/initialize-equity 100000.0) ;; hardcoding for now
  (let [max-pos (oms/determine-notionals @pairs)]
    (log/info (format "max notional per pair: %f" max-pos))
    (log/info (format "expected abs position size, pre-vol scale: %f"
                      (model/set-scale-target max-pos))))
  (log/info "Starting md handlers...")
  (start-md-handlers model/handle-trade trade-channels)
  (start-md-distributor)
  (start-md-main)
  (log/info "Subscribing to market data...")
  (api/ftx-subscribe-all ftx-ws-conn :trades @pairs)
  (log/info "Started!")
  (.await signal))

(defn -main
  [& args]
  (run))
