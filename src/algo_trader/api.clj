(ns algo-trader.api
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [algo-trader.config :refer [config]]
            [cheshire.core :refer [generate-string parse-string]]
            [clojure.core.async :refer [<! go-loop timeout]]
            [clojure.string :refer [includes?]]
            [java-time :refer [as duration instant]]
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

(defn- ftx-subscription-manage [conn chan market op]
  (s/put! conn (generate-string {:op op :channel chan :market market})))

(defn ftx-subscribe [conn chan market]
  (ftx-subscription-manage conn chan market :subscribe))

(defn ftx-unsubscribe [conn chan market]
  (ftx-subscription-manage conn chan market :unsubscribe))

(defn ftx-subscribe-all [conn chan markets]
  (doseq [market markets]
    (ftx-subscribe conn chan market)))

(defn ftx-get [path params]
  (http/get (str (:ftx-api config) path)
            {:query-params params}))

(defn ftx-us-get [path params]
  (http/get (str (:ftx-api-us config) path)
            {:query-params params}))

(defn ftx [path params]
  (-> @(ftx-get path params)
      :body
      bs/to-string
      (parse-string true)))

(defn ftx-us [path params]
  (-> @(ftx-us-get path params)
      :body
      bs/to-string
      (parse-string true)))

(defn get-futures-targets [underlying-set]
  (let [all-markets (->> (ftx "/futures" {})
                         :result
                         (filter #(includes? (:name %) "-PERP")))
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

(defn get-market-info
  "fetch spot market info, but map to perps to match market data"
  [underlying-set]
  (let [all-markets (->> (ftx-us "/markets" {})
                         :result
                         (map #(update % :quoteCurrency keyword))
                         (filter #(= :USD (:quoteCurrency %))))
        filtered (if (empty? underlying-set)
                   (->> all-markets
                        (sort-by :volumeUsd24h)
                        reverse
                        (take (:num-markets config)))
                   (->> all-markets
                        (filter #(underlying-set (keyword (:baseCurrency %))))))]
    (reduce
     (fn [acc {:keys [baseCurrency] :as m}]
       ;; here is the hacky bit
       (assoc acc (keyword (str baseCurrency "-PERP")) m))
     {}
     filtered)))

(defn fetch-for-db
  [path params]
  (map
   (fn [x]
     (-> (update x :side #(if (= "buy" %) "b" "s"))
         (update :time instant)))
   (:result (ftx path params))))
