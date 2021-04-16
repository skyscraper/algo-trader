(ns algo-trader.oms
  (:require [algo-trader.config :refer [config]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [log-rtn]]
            [clojure.core.async :refer [<! go-loop]]))

(def oms-channels {})
(def positions {})
(def equity (atom 0.0))
(def min-order-notional (atom 0.0))
(def min-pos-notional (atom 0.0))
(def max-pos-notional (atom 0.0))

(defn initialize-equity [eq]
  (reset! equity eq))

(def starting-cash 30000.0)
(defn initialize-positions [markets]
  (alter-var-root
   #'positions
   #(reduce
    (fn [acc x]
      (assoc acc x (atom {:cash starting-cash :shares 0.0 :port-val starting-cash})))
    %
    markets)))

(defn determine-notionals [markets]
  (let [eq (/ (* 0.95 @equity) (count markets)) ;; divide 95% of equity evenly
        max-notional (min (:max-pos-notional config) eq)] ;; take min of split and hard limit
    (reset! min-order-notional (* (:min-order-notional-percent config) max-notional))
    (reset! min-pos-notional (* (:min-pos-notional-percent config) max-notional))
    (reset! max-pos-notional max-notional))) ;; last to return max

(defn get-bounded-target [target-notional]
  (let [abs-notional (Math/abs target-notional)
        op (if (< target-notional 0.0) - +)]
    (cond
      (> abs-notional @max-pos-notional) (op @max-pos-notional)
      (< abs-notional @min-pos-notional) 0.0
      :else target-notional)))

;; temp crude logic for "live" backtesting - just send estimate of portfolio rtn to statsd
(defn handle-target [{:keys [market price target side]} p]
  (let [target (get-bounded-target target)
        {:keys [port-val]}
        (swap!
         p
         (fn [{:keys [cash shares]}]
           (let [pos-mtm (* shares price)
                 delta-cash (- target pos-mtm)]
             (if (>= (Math/abs delta-cash) @min-order-notional)
               (let [new-cash (- cash delta-cash)
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
                     port-val (+ new-cash (* new-shares slip-price))]
                 {:cash new-cash :shares new-shares :port-val port-val})
               {:cash cash :shares shares :port-val (+ cash (* shares price))}))))]
    (statsd/gauge :port-val (log-rtn starting-cash port-val) (list market))))

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
