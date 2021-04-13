(ns algo-trader.api
  (:require [aleph.http :as http]
            [algo-trader.config :refer [config]]
            [cheshire.core :refer [generate-string]]
            [clojure.core.async :refer [<! go-loop timeout]]
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

(defn- ftx-subscription-manage [conn chan pair op]
  (s/put! conn (generate-string {:op op :channel chan :market pair})))

(defn ftx-subscribe [conn chan pair]
  (ftx-subscription-manage conn chan pair :subscribe))

(defn ftx-unsubscribe [conn chan pair]
  (ftx-subscription-manage conn chan pair :unsubscribe))

(defn ftx-subscribe-all [conn chan pairs]
  (doseq [pair pairs]
    (ftx-subscribe conn chan pair)))
