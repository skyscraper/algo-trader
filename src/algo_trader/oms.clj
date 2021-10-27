(ns algo-trader.oms
  (:require [algo-trader.api :as api]
            [algo-trader.config :refer [config price-slippage fee-mults]]
            [algo-trader.statsd :as statsd]
            [algo-trader.utils :refer [pct-rtn vol-scalar]]
            [clojure.core.async :refer [<! go-loop]]
            [java-time :refer [instant]]
            [taoensso.timbre :as log]))

(def non-inflight-statuses #{:closed :local-cancel})
(def oms-channels {})
(def positions {})
(def equity (atom 0.0))
(def starting-cash (atom 0.0))                              ;; testing

(defn initialize-equity [eq]
  (reset! equity eq))

(defn initialize-positions [markets starting-cash]
  (alter-var-root
    #'positions
    #(reduce
       (fn [acc x]
         (assoc acc x (atom {:cash            starting-cash
                             :shares          0.0
                             :port-val        starting-cash
                             :order-id        nil
                             :order-inflight? false
                             :order-submit-ts nil})))
       %
       markets)))

(defn determine-notionals [markets]
  (reset! starting-cash (/ @equity (count markets))))

(defn get-target
  "block size is the min size increment for the underlying (ftx.us)
   vol-scale is the number of blocks to get to the per-bar cash vol target
   thus, we need to FIRST round to get to integer number of blocks, and then
   adjust to the actual decimal"
  [market forecast port-mtm price vol]
  (let [block-size (get-in config [:block-sizes market])
        vol-scale (vol-scalar port-mtm price block-size vol)]
    (* block-size (Math/round (/ (* forecast vol-scale) (:scale-target config))))))

(defn handle-target
  [{:keys [market price forecast vol]} p]
  (let [{:keys [cash shares]} @p
        port-mtm (+ (* shares price) cash)
        target (get-target market forecast port-mtm price vol)
        delta-shares (- target shares)
        thresh (Math/abs (* (:position-inertia-percent config) shares))]
    (when (>= (Math/abs delta-shares) thresh)
      (let [ord-id (str (java.util.UUID/randomUUID))
            qty (Math/abs delta-shares)
            side (if (neg? delta-shares) :sell :buy)]
        (swap! p assoc
               :order-id ord-id
               :order-inflight? true
               :order-submit-ts (System/currentTimeMillis))
        (api/market-order-async side qty market ord-id (market oms-channels))
        (log/info (format "%s submitting TARGET %s %s %d" ord-id (name side) market qty))
        (statsd/count :order-generated 1 (list market))))))

(defn handle-response
  [{:keys [createdAt market status clientId]} p]
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

(defn handle-fill
  "updates portfolio values"
  [{:keys [market fee price size] :as msg} p]
  (let [side (keyword (:side msg))
        match-time (.toEpochMilli (instant (:time msg)))
        delta-cash (* size price (if (= side :buy) 1 -1))
        delta-shares (if (= side :buy) size (- size))
        {:keys [port-val order-submit-ts]}
        (swap!
          p
          (fn [{:keys [cash shares] :as x}]
            (let [new-cash (- cash delta-cash fee)
                  new-shares (+ shares delta-shares)
                  pos-val (* new-shares price)
                  port-val (+ pos-val new-cash)]
              (assoc x :cash new-cash :shares new-shares :port-val port-val))))]
    (log/info (format "received FILL %s %d @ $%d" (name market) size price))
    (statsd/gauge :port-val (pct-rtn @starting-cash port-val) (list market))
    (statsd/distribution
      :match-time
      (- match-time order-submit-ts)
      nil)
    (statsd/count :fill-received 1 (list symbol))))

(defn handle-order-update
  "updates order status"
  [{:keys [clientId status filledSize remainingSize]} p]
  (let [{:keys [order-id]} @p
        status-kw (keyword status)]
    (log/info
      (format "%s received UPDATE %s, filled %d, remaining: %d"
              clientId status filledSize remainingSize))
    (if (= clientId order-id)
      (if (:closed (keyword status-kw))
        (swap! p assoc :status status-kw :order-inflight? false)
        (swap! p assoc :status status-kw))
      (log/warn (format "received mismatched UPDATE for order %s but postion has %s"
                        clientId order-id)))))

(defn handle-oms-data
  [sym {:keys [msg-type] :as msg}]
  (let [p (sym positions)]
    (condp = msg-type
      :target
      (handle-target msg p)
      :response
      (handle-response msg p)
      :fills
      (handle-fill msg p)
      :orders
      (handle-order-update msg p)
      (log/warn "unhandled oms msg-type: " msg-type))))

(defn start-oms-handlers [channels]
  (alter-var-root #'oms-channels merge channels)
  (doseq [[sym ch] channels]
    (go-loop []
      (handle-oms-data sym (<! ch))
      (recur))))

;;; paper trading logic ;;;

(defn slipped-price [price side delta]
  ;; if we are going same way, only 1bps slippage, otherwise 5bps to cross spread
  (let [i (if (pos? delta)
            (if (= :buy side) 0 1)
            (if (= :buy side) 2 3))]
    (* price (nth price-slippage i))))

(defn update-paper-port! [market price forecast side vol p]
  (:port-val
    (swap!
      p
      (fn [{:keys [cash shares]}]
        (let [port-mtm (+ (* shares price) cash)
              target (get-target market forecast port-mtm price vol)
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
  (let [port-val (update-paper-port! market price forecast side vol p)]
    (statsd/gauge :port-val (pct-rtn @starting-cash port-val) (list market))))

(defn handle-paper-data [sym {:keys [msg-type] :as msg}]
  (let [p (sym positions)]
    (condp = msg-type
      :target
      (handle-paper-target msg p)
      nil)))

(defn start-paper-handlers [channels]
  (alter-var-root #'oms-channels merge channels)
  (doseq [[sym ch] channels]
    (go-loop []
      (handle-paper-data sym (<! ch))
      (recur))))
