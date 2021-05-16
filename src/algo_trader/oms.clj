(ns algo-trader.oms
  (:require [algo-trader.config :refer [config]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [pct-rtn vol-scalar]]
            [clojure.core.async :refer [<! go-loop]]))

(def oms-channels {})
(def positions {})
(def equity (atom 0.0))
(def starting-cash (atom 0.0)) ;; testing

(defn initialize-equity [eq]
  (reset! equity eq))

(defn initialize-positions [markets starting-cash]
  (alter-var-root
   #'positions
   #(reduce
    (fn [acc x]
      (assoc acc x (atom {:cash starting-cash :shares 0.0 :port-val starting-cash})))
    %
    markets)))

(defn determine-notionals [markets]
  (reset! starting-cash (/ @equity (count markets))))

(defn get-target [target current-eq]
  (let [mult (/ current-eq (:scale-cap config))]
    (min (* mult target) (:max-pos-notional config))))

(defn update-port! [price forecast side vol p]
  (:port-val
   (swap!
    p
    (fn [{:keys [cash shares]}]
      (let [pos-mtm (* shares price)
            port-mtm (+ pos-mtm cash)
            block-size 1.0 ;; purposefully hardcoded; see Carver 154
            vol-scale (vol-scalar port-mtm price block-size vol)
            target (/ (* forecast vol-scale) (:scale-target config))
            delta-shares (- target shares)
            thresh (Math/abs (* (:position-inertia-percent config) shares))]
        (if (>= (Math/abs delta-shares) thresh)
          (let [;; if we are going same way, only 1bps slippage, otherwise 5bps to cross spread
                slip-price (if (pos? delta-shares)
                             (if (= :buy side)
                               (* price 1.0001)
                               (* price 1.0005))
                             (if (= :buy side)
                               (* price 0.9995)
                               (* price 0.9999)))
                ;; ftx taker fees are 7bps for lowest tier
                my-price (if (pos? delta-shares)
                           (* slip-price 1.0007)
                           (* slip-price 0.9993))
                delta-cash (* delta-shares my-price)
                new-cash (- cash delta-cash)
                pos-val (* target slip-price)
                port-val (+ pos-val new-cash)]
            {:cash new-cash :shares target :port-val port-val})
          {:cash cash :shares shares :port-val port-mtm}))))))

;; temp crude logic for "live" backtesting - just send estimate of portfolio rtn to statsd
(defn handle-target [{:keys [market price forecast side vol]} p]
  (let [port-val (update-port! price forecast side vol p)]
    (statsd/gauge :port-val (pct-rtn @starting-cash port-val) (list market))))

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
