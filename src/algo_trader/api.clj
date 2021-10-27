(ns algo-trader.api
  (:require [aleph.http :as http]
            [algo-trader.config :refer [config orders-ep]]
            [algo-trader.utils :refer [spot-kw]]
            [byte-streams :as bs]
            [clojure.core.async :refer [put!]]
            [java-time :refer [instant]]
            [jsonista.core :as json]
            [manifold.deferred :as d]
            [taoensso.timbre :as log])
  (:import (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)
           (org.apache.commons.codec.binary Hex)))

(def ftx-api "https://ftx.com/api")
(def ftx-us-api "https://ftx.us/api")
(def algo "HmacSHA256")
(def fmt "UTF-8")
(def signing-key (SecretKeySpec. (.getBytes (:ftx-us-secret-key config) fmt) algo))
(def ws-str "%swebsocket_login")
(def api-str "%s%s%s%s")
(def empty-str "")
(def reqs {:GET    http/get
           :POST   http/post
           :DELETE http/delete})

(defn hmac
  "Calculate HMAC signature for given data."
  [^String data]
  (let [mac (doto (Mac/getInstance algo) (.init signing-key))]
    (Hex/encodeHexString (.doFinal mac (.getBytes data ^String fmt)))))

(defn ws-sig [ts]
  (format ws-str ts))

(defn api-sig [ts method path body]
  (format api-str ts method path body))

(defn auth-headers [ts req-kw path body]
  {:FTXUS-KEY  (:ftx-us-api-key config)
   :FTXUS-SIGN (hmac (api-sig ts (name req-kw) path (or body empty-str)))
   :FTXUS-TS   (str ts)})

(defn ftx [req-kw path body params auth?]
  (-> @((req-kw reqs)
        (str ftx-api path)
        {:headers      (when auth? (auth-headers (System/currentTimeMillis) req-kw path nil))
         :query-params params
         :body         (when body (bs/to-byte-array (json/write-value-as-string body)))})
      :body
      bs/to-string
      (json/read-value json/keyword-keys-object-mapper)))

(defn ftx-us-async [req-kw path body auth? success-fn failure-fn]
  (let [body-str (when body (json/write-value-as-string body))
        body-bs (when body-str (bs/to-byte-array body-str))
        resp ((req-kw reqs)
              (str ftx-us-api path)
              {:headers
                     (when auth?
                       (auth-headers (System/currentTimeMillis) req-kw path body-str))
               :body body-bs})]
    (d/on-realized resp success-fn failure-fn)))

(def problem-response {:msg-type :response :status :local-cancel})

(defn order-async [ioc? tif side qty price market ord-id response-chan]
  (let [payload {:market   (spot-kw market)
                 :side     side
                 :price    price
                 :type     tif
                 :size     qty
                 :ioc      ioc?
                 :clientId ord-id}
        default (assoc problem-response :market market :clientId ord-id)
        success-fn
        (fn [{:keys [body]}]
          (try
            (->> body
                 bs/to-string
                 (#(json/read-value % json/keyword-keys-object-mapper))
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
            (let [b (-> body bs/to-string (json/read-value json/keyword-keys-object-mapper))]
              (log/error (format "async market order status %d for %s: %s"
                                 status (:market default) (:message b))))
            (log/error "async market order:" ex)))]
    (ftx-us-async :POST orders-ep payload true success-fn failure-fn)))

(defn market-order-async [side qty market ord-id response-chan]
  (order-async :market false side qty nil market ord-id response-chan))

(defn ioc-async [side qty price market ord-id response-chan]
  (order-async :limit true side qty price market ord-id response-chan))

(defn fetch-for-db
  [path params]
  (map
    (fn [x]
      (-> (update x :side #(if (= "buy" %) "b" "s"))
          (update :time instant)))
    (:result (ftx :GET path nil params false))))
