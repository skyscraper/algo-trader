(ns algo-trader.oms
  (:require [algo-trader.api :as api]
            [algo-trader.config :refer [config price-slippage fee-mults]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [pct-rtn vol-scalar]]
            [clojure.core.async :refer [<! go-loop]]
            [clojure.tools.logging :as log]
            [java-time :refer [instant]]))

(def non-inflight-statuses #{:closed :local-cancel})
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
      (assoc acc x (atom {:cash starting-cash
                          :shares 0.0
                          :port-val starting-cash
                          :order-id nil
                          :order-inflight? false
                          :order-submit-ts nil})))
    %
    markets)))

(defn determine-notionals [markets]
  (reset! starting-cash (/ @equity (count markets))))

(defn get-target [forecast port-mtm price vol]
  (let [block-size 1.0 ;; purposefully hardcoded; see Carver 154
        vol-scale (vol-scalar port-mtm price block-size vol)]
    (/ (* forecast vol-scale) (:scale-target config))))

(defn handle-target [{:keys [market price forecast side vol]} p]
  (let [{:keys [cash shares]} @p
        port-mtm (+ (* shares price) cash)
        target (get-target forecast port-mtm price vol)
        delta-shares (- target shares)
        thresh (Math/abs (* (:position-inertia-percent config) shares))]
    (when (>= (Math/abs delta-shares) thresh)
      (let [ord-id (str (java.util.UUID/randomUUID))
            qty (Math/abs delta-shares)]
        (swap! p assoc
               :order-id ord-id
               :order-inflight? true
               :order-submit-ts (System/currentTimeMillis))
        (api/market-order-async side qty market ord-id (market oms-channels))
        (log/info (format "%s submitting TARGET %s %s %d" ord-id (name side) market qty))
        (statsd/count :order-generated 1 (list market))))))

(defn handle-response [{:keys [createdAt market status clientId]} p]
  (let [{:keys [order-submit-ts order-id]} @p
        kw-status (keyword status)]
    (log/info (format "%s received RESPONSE %s" clientId status))
    (if (= clientId order-id)
      (if (non-inflight-statuses kw-status)
        (swap! p assoc :event kw-status :order-inflight? false)
        (do
          (swap! p assoc :event kw-status)
          (let [order-create-time (.toEpochMilli (instant createdAt))]
            (statsd/distribution
             :order-submit-time
             (- order-create-time order-submit-ts)
             (list market)))))
      (log/warn (format "received mismatched RESPONSE for order %s but postion has %s"
                        clientId order-id)))))

(defn handle-fill [msg p]
  ;; todo
  nil)

(defn handle-update [msg p]
  ;; todo
  nil)

(defn handle-oms-data [{:keys [msg-type market] :as msg}]
  (let [p (market positions)]
    (condp = msg-type
      :target
      (handle-target msg p)
      :response
      (handle-response msg p)
      :fill
      (handle-fill msg p)
      :update
      (handle-update msg p)
      nil)))

(defn start-oms-handlers [channel-map]
  (alter-var-root #'oms-channels merge channel-map)
  (doseq [c (vals channel-map)]
    (go-loop []
      (when-let [x (<! c)]
        (handle-oms-data x)
        (recur)))))

;;; paper trading logic ;;;

(defn slipped-price [price side delta]
  ;; if we are going same way, only 1bps slippage, otherwise 5bps to cross spread
  (let [i (if (pos? delta)
            (if (= :buy side) 0 1)
            (if (= :buy side) 2 3))]
    (* price (nth price-slippage i))))

(defn update-paper-port! [price forecast side vol p]
  (:port-val
   (swap!
    p
    (fn [{:keys [cash shares]}]
      (let [port-mtm (+ (* shares price) cash)
            target (get-target forecast port-mtm price vol)
            delta-shares (- target shares)
            thresh (Math/abs (* (:position-inertia-percent config) shares))]
        (if (>= (Math/abs delta-shares) thresh)
          (let [slip-price (slipped-price price side delta-shares)
                my-price (if (pos? delta-shares)
                           (* slip-price (first fee-mults))
                           (* slip-price (last fee-mults)))
                delta-cash (* delta-shares my-price)
                new-cash (- cash delta-cash)
                pos-val (* target slip-price)
                port-val (+ pos-val new-cash)]
            {:cash new-cash :shares target :port-val port-val})
          {:cash cash :shares shares :port-val port-mtm}))))))

(defn handle-paper-target [{:keys [market price forecast side vol]} p]
  (let [port-val (update-paper-port! price forecast side vol p)]
    (statsd/gauge :port-val (pct-rtn @starting-cash port-val) (list market))))

(defn handle-paper-data [{:keys [msg-type market] :as msg}]
  (let [p (market positions)]
    (condp = msg-type
      :target
      (handle-paper-target msg p)
      nil)))

(defn start-paper-handlers [channel-map]
  (alter-var-root #'oms-channels merge channel-map)
  (doseq [c (vals channel-map)]
    (go-loop []
      (when-let [x (<! c)]
        (handle-paper-data x)
        (recur)))))
