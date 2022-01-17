(ns algo-trader.core
  (:gen-class)
  (:require [algo-trader.api :refer [ftx-us-sync]]
            [algo-trader.db :as db]
            [algo-trader.backtest :as backtest]
            [algo-trader.config :refer [config wallet-ep hardcoded-eq]]
            [algo-trader.handlers
             [binance :as binance]
             [binance-inv :as binance-inv]
             [bybit :as bybit]
             [bybit-inv :as bybit-inv]
             [deribit :as deribit]
             [ftx :as ftx]
             [ftx-us :as ftx-us]
             [huobi :as huobi]
             [huobi-inv :as huobi-inv]
             [kraken-inv :as kraken-inv]
             [okex :as okex]]
            [algo-trader.model :as model]
            [algo-trader.oms :as oms]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [generate-channel-map get-target-sizes uc-kw market-kw]]
            [clojure.core.async :refer [<! go-loop]]
            [clojure.string :refer [join]]
            [clojure.tools.cli :as cli]
            [taoensso.timbre :as log])
  (:import (java.util.concurrent CountDownLatch)))

(def markets (atom nil))
(def trade-channels {})
(def signal (CountDownLatch. 1))

(defn get-ftx-balances []
  (let [res (:result (ftx-us-sync :GET wallet-ep nil true (:subaccount config)))]
    (reduce
     (fn [acc {:keys [coin] :as x}]
       (assoc acc (keyword coin) x))
     {}
     res)))

(defn start-md-handlers
  "Starts a go-loop and applies a fn to every event received on a given channel.
   Event types should be homogenous."
  [f channels]
  (doseq [[sym ch] channels]
    (go-loop []
      (f sym (<! ch))
      (recur))))

(def inits
  [binance/init
   binance-inv/init
   bybit/init
   bybit-inv/init
   ;deribit/init ; seems to require auth now
   ftx/init
   huobi/init
   huobi-inv/init
   kraken-inv/init
   okex/init])

(defn paper-setup []
  (log/info "Initializing positions...")
  (oms/initialize-positions @markets {:USD {:free hardcoded-eq}})
  (oms/determine-starting-capital-per-market)
  (log/info "Starting OMS handlers...")
  (oms/start-paper-handlers (generate-channel-map @markets)))

(defn prod-setup []
  (log/info "Initializing positions...")
  (oms/initialize-positions @markets (get-ftx-balances))
  (oms/determine-starting-capital-per-market)
  (log/info "Subscribing to orders and fills...")
  (oms/start-oms-handlers (generate-channel-map @markets))
  (log/info "Starting OMS handlers...")
  (ftx-us/init oms/oms-channels))

(defn run
  [paper?]
  (log/swap-config! assoc :appenders {:spit (log/spit-appender {:fname "./logs/app.log"})})
  (log/set-level! :info)
  (log/info (format "Starting trader in %s mode" (if paper? "PAPER" "PRODUCTION")))
  (log/info "Connecting to statsd...")
  (statsd/reset-statsd!)
  (let [target-sizes (get-target-sizes)]
    (reset! markets (keys target-sizes))
    (model/initialize target-sizes)
    (doseq [[market target] target-sizes]
      (log/info (format "%s target: %,d" (name market) target))))
  (log/info "Today we will be trading:" (join ", " (map name @markets)))
  (alter-var-root #'trade-channels merge (generate-channel-map @markets))
  (if paper?
    (paper-setup)
    (prod-setup))
  (log/info "Starting market data handlers...")
  (start-md-handlers model/handle-trade trade-channels)
  (log/info "Connecting to exchanges and subscribing to market data...")
  (doseq [init inits]
    (init trade-channels))
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
      collect? nil                                          ;; todo
      historical? (db/fetch-and-store (market-kw underlying) lookback append?)
      :else (run papertrading?))))
