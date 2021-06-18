(ns algo-trader.api
  (:require [aleph.http :as http]
            [algo-trader.config :refer [config orders-ep futures-ep]]
            [algo-trader.utils :refer [spot-kw]]
            [byte-streams :as bs]
            [cheshire.core :refer [generate-string parse-string]]
            [clojure.core.async :refer [<! put! go-loop timeout]]
            [clojure.tools.logging :as log]
            [java-time :refer [as duration instant]]
            [manifold.deferred :as d]
            [manifold.stream :as s])
  (:import (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)
           (org.apache.commons.codec.binary Hex)))

(def algo "HmacSHA256")
(def fmt "UTF-8")
(def signing-key (SecretKeySpec. (.getBytes (:ftx-secret-key config) fmt) algo))
(def ping (generate-string {:op :ping}))
(def ws-str "%swebsocket_login")
(def api-str "%s%s%s%s")
(def empty-str "")
(def reqs {:GET http/get
           :POST http/post
           :DELETE http/delete})

(defn hmac
  "Calculate HMAC signature for given data."
  [^String data]
  (let [mac (doto (Mac/getInstance algo) (.init signing-key))]
    (Hex/encodeHexString (.doFinal mac (.getBytes data fmt)))))

(defn ping-loop [conn]
  (go-loop []
    (<! (timeout 15000))
    (when @(s/put! conn ping)
      (recur))))

(defn ws-sig [ts]
  (format ws-str ts))

(defn api-sig [ts method path body]
  (format api-str ts method path body))

(defn auth-headers [ts req-kw path body]
  {:FTXUS-KEY (:ftx-api-key config)
   :FTXUS-SIGN (hmac (api-sig ts (name req-kw) path (or body empty-str)))
   :FTXUS-TS (str ts)})

(defn ftx-websocket [url]
  (let [conn @(http/websocket-client url {:epoll? true})]
    (ping-loop conn)
    conn))

(defn ftx-login [conn]
  (let [now (System/currentTimeMillis)
        sig (hmac (ws-sig now))]
    (s/put! conn (generate-string {:op :login
                                   :args {:key (:ftx-api-key config)
                                          :sign sig
                                          :time now}}))))

(defn- ftx-subscription-manage [conn chan market op]
  (s/put! conn (generate-string {:op op :channel chan :market market})))

(defn ftx-subscribe [conn chan market]
  (ftx-subscription-manage conn chan market :subscribe))

(defn ftx-unsubscribe [conn chan market]
  (ftx-subscription-manage conn chan market :unsubscribe))

(defn ftx-subscribe-all [conn chan markets]
  (doseq [market markets]
    (ftx-subscribe conn chan market)))

(defn ftx [req-kw path body params auth?]
  (-> @((req-kw reqs)
        (str (:ftx-api config) path)
        {:headers (when auth?
                    (auth-headers (System/currentTimeMillis) req-kw path nil))
         :query-params params
         :body (when body
                 (bs/to-byte-array (generate-string body)))})
      :body
      bs/to-string
      (parse-string true)))

(defn ftx-async-us [req-kw path body auth? success-fn failure-fn]
  (let [body-str (when body (generate-string body))
        body-bs (when body-str (bs/to-byte-array body-str))
        resp ((req-kw reqs)
              (str (:ftx-api-us config) path)
              {:headers
               (when auth?
                 (auth-headers (System/currentTimeMillis) req-kw path body-str))
               :body body-bs})]
    (d/on-realized resp success-fn failure-fn)))

(def problem-response {:msg-type :response :status :local-cancel})

(defn order-async [ioc? tif side qty price market ord-id response-chan]
  (let [payload {:market (spot-kw market)
                 :side side
                 :price price
                 :type tif
                 :size qty
                 :ioc ioc?
                 :clientId ord-id}
        default (assoc problem-response :market market :clientId ord-id)
        success-fn
        (fn [{:keys [body]}]
          (try
            (->> body
                 bs/to-string
                 (#(parse-string % true))
                 :result
                 (merge default)
                 (merge {:market market})
                 (put! response-chan))
            (catch Exception e
              (put! response-chan default)
              (log/error "order request successful, problem handling response:" e))))
        failure-fn
        (fn [ex]
          (put! response-chan default)
          (if-let [{:keys [status body]} (ex-data ex)]
            (let [b (-> body bs/to-string (parse-string true))]
              (log/error (format "async market order status %d for %s: %s"
                                 status (:market default) (:message b))))
            (log/error "async market order:" ex)))]
    (ftx-async-us :POST orders-ep payload true success-fn failure-fn)))

(defn market-order-async [side qty market ord-id response-chan]
  (order-async :market false side qty nil market ord-id response-chan))

(defn ioc-async [side qty price market ord-id response-chan]
  (order-async :limit true side qty price market ord-id response-chan))

(defn get-futures-targets [underlying-set]
  (let [all-markets (->> (ftx :GET futures-ep nil {} false)
                         :result
                         (filter #(= (:type %) "perpetual")))
        filtered (if (empty? underlying-set)
                   (->> all-markets
                        (sort-by :volumeUsd24h)
                        reverse
                        (take (:num-markets config)))
                   (->> all-markets
                        (map #(update % :underlying keyword))
                        (filter #(underlying-set (:underlying %)))))
        frac (/ (:est-bar-mins config) (as (duration 1 :days) :minutes))]
    (reduce
     (fn [acc {:keys [name volumeUsd24h]}]
       (assoc acc (keyword name) (* frac volumeUsd24h)))
     {}
     filtered)))

(defn fetch-for-db
  [path params]
  (map
   (fn [x]
     (-> (update x :side #(if (= "buy" %) "b" "s"))
         (update :time instant)))
   (:result (ftx :GET path nil params false))))
