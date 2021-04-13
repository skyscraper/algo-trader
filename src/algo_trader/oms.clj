(ns algo-trader.oms
  (:require [algo-trader.config :refer [config]]
            [algo-trader.statsd :as statsd]
            [clojure.core.async :refer [<! go-loop]]))

(def oms-channels {})
(def positions {})
(def equity (atom 0.0))
(def max-pos-notional (atom 0.0))
(def min-pos-notional (atom 0.0))

(defn initialize-equity [eq]
  (reset! equity eq))

(defn initialize-positions [markets]
  (alter-var-root
   #'positions
   #(reduce
    (fn [acc x]
      (assoc acc x (atom {:cash 10000.0 :shares 0.0 :port-val 10000.0})))
    %
    markets)))

(defn get-max-notional [equity n-markets]
  (let [eq (/ (* 0.95 equity) n-markets)] ;; divide 95% of equity evenly
    (min (:max-pos-notional config) eq))) ;; take min of split and hard limit

(defn get-min-notional [max-notional]
  (* (:min-pos-notional-percent config) max-notional))

(defn determine-notionals [markets]
  (let [max-notional (get-max-notional @equity (count markets))]
    (reset! min-pos-notional (get-min-notional max-notional))
    (reset! max-pos-notional max-notional))) ;; last to return max

;; temp crude logic for "live" backtesting - just send estimate of port value to statsd
(defn handle-target [{:keys [market price target side]} p]
  (let [{:keys [port-val]}
        (swap! p
               (fn [{:keys [cash shares]}]
                 (let [pos-mtm (* shares price)
                       delta-cash (- target pos-mtm)
                       new-cash (- cash delta-cash)
                       ;; if we are going same way, only 1bps slippage, otherwise 5bps to cross spread
                       slip-price (if (pos? delta-cash)
                                    (if (= :buy side)
                                      (* price 1.0001)
                                      (* price 1.0005))
                                    (if (= :buy side)
                                      (* price 0.9995)
                                      (* price 0.9999)))
                       ;; ftx taker fees are 7bps for lowest tier
                       my-price (if (pos? delta-cash)
                                  (* slip-price 1.0007)
                                  (* slip-price 0.9993))
                       delta-shares (/ delta-cash my-price)
                       new-shares (+ shares delta-shares)
                       port-val (+ cash (* shares slip-price))]
                   {:cash new-cash :shares new-shares :port-val port-val})))]
    (statsd/gauge :port-val port-val (list market))))

(defn handle-oms-data [{:keys [msg-type market] :as msg}]
  (let [p (market positions)]
    (condp = msg-type
      :target
      (handle-target msg p)
      nil)))

(defn start-oms-handlers [channel-map]
  (alter-var-root #'oms-channels merge channel-map)
  (doseq [c (vals channel-map)]
    (go-loop []
      (when-let [x (<! c)]
        (handle-oms-data x)
        (recur)))))
