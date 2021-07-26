(ns algo-trader.core
  (:gen-class)
  (:require [algo-trader.api :as api]
            [algo-trader.db :as db]
            [algo-trader.backtest :as backtest]
            [algo-trader.config :refer [config hardcoded-eq]]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [generate-channel-map get-target-amts uc-kw market-kw
                                       market-from-spot]]
            [cheshire.core :refer [parse-string]]
            [clojure.core.async :refer [<! >! chan go go-loop]]
            [clojure.string :refer [join]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [manifold.deferred :refer [timeout!]]
            [manifold.stream :refer [consume take!]]))

(def markets (atom nil))
(def ftx-ws-conn nil)
(def ftx-us-ws-conn nil)
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
  (api/ftx-subscribe-all-markets ftx-ws-conn :trades @markets))

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
        :pong (log/debug "pong")
        (log/warn (str "unhandled ftx event: " payload)))
      (recur))))

(defn reset-ftx-us!
  "reset ftx-us ws connection"
  []
  (log/info "Connecting to orders and fills...")
  (alter-var-root
   #'ftx-us-ws-conn
   (fn [_] (api/ftx-websocket (:ftx-us-ws config)))))

(defn handle-orders-fills
  [msg]
  (go
    (let [{:keys [channel data]} (-> (parse-string msg true)
                                     (update :channel keyword)
                                     (update-in [:data :market] market-from-spot))]
      (>! ((:market data) oms/oms-channels) (assoc data :msg-type channel)))))

(defn start-order-fill-handler
  "Orders/fills consumer"
  []
  (consume handle-orders-fills ftx-us-ws-conn))

(defn start-md-handlers
  "Starts a go-loop and applies a fn to every event received on a given channel.
   Event types should be homogenous."
  [f channels]
  (doseq [c (vals channels)]
    (go-loop []
      (f (<! c))
      (recur))))

(defn paper-setup []
  (reset-ftx-us!)
  (oms/start-paper-handlers (generate-channel-map @markets))
  (log/info "Subscribing to orders and fills...")
  (api/ftx-login ftx-us-ws-conn)
  (api/ftx-subscribe ftx-us-ws-conn :orders)
  (api/ftx-subscribe ftx-us-ws-conn :fills))

(defn run
  [paper?]
  (log/info (format "Starting trader in %s mode" (if paper? "PAPER" "PRODUCTION")))
  (log/info "Connecting to statsd...")
  (statsd/reset-statsd!)
  (log/info "Connecting to ftx...")
  (reset-ftx!)
  (let [target-amts (get-target-amts)]
    (reset! markets (keys target-amts))
    (model/initialize target-amts)
    (doseq [[market target] target-amts]
      (log/info (format "%s target: %,.2f" (name market) target))))
  (log/info "Today we will be trading:" (join ", " (map name @markets)))
  (alter-var-root #'trade-channels merge (generate-channel-map @markets))
  (log/info "Initializing positions...")
  (oms/initialize-equity hardcoded-eq)
  (let [starting-cash (oms/determine-notionals @markets)]
    (log/info (format "starting equity per market: %,.2f" starting-cash))
    (oms/initialize-positions @markets starting-cash))
  (log/info "Starting OMS handlers...")
  (if paper?
    (paper-setup)
    (oms/start-oms-handlers (generate-channel-map @markets)))
  (log/info "Starting market data handlers...")
  (start-md-handlers model/handle-trade trade-channels)
  (start-md-distributor)
  (start-md-main)
  (log/info "Subscribing to market data...")
  (api/ftx-subscribe-all-markets ftx-ws-conn :trades @markets)
  (log/info "Started!")
  (.await signal))

(def cli-options
  [["-p" nil "Paper trading"
    :id :papertrading?
    :default false]
   ["-b" nil "Backtest"
    :id :backtest?
    :default false]
   ["-c" nil "Collect real time data"
    :id :collect?
    :default false]
   ["-h" nil "Fetch historical data"
    :id :historical?
    :default false]
   ["-u" "--underlying UNDERLYING" "Underlying token"
    :id :underlying
    :default :ETH
    :parse-fn uc-kw]
   ["-l" "--lookback LOOKBACK" "Lookback days to fetch"
    :id :lookback
    :default 1.0
    :parse-fn #(Double/parseDouble %)]
   ["-a" nil "Append historical data"
    :id :append?
    :default false]])

(defn -main
  [& args]
  (let [{:keys [papertrading? backtest? collect? historical? underlying lookback append?]}
        (:options (cli/parse-opts args cli-options))]
    (cond
      backtest? (backtest/run-all)
      collect? nil ;; todo
      historical? (db/fetch-and-store (market-kw underlying) lookback append?)
      :else (run papertrading?))))
