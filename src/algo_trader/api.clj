(ns algo-trader.api
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [algo-trader.config :refer [config minutes-in-day]]
            [algo-trader.utils :refer [epoch]]
            [cheshire.core :refer [generate-string parse-string]]
            [clojure.core.async :refer [<! go-loop timeout]]
            [clojure.set :refer [union rename-keys]]
            [clojure.string :refer [includes?]]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [java-time :refer [instant]]
            [manifold.stream :as s]
            [next.jdbc :as jdbc]
            [next.jdbc.date-time]
            [taoensso.nippy :as nippy])
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

(defn get-futures-targets [markets]
  (let [all-markets (->> (ftx "/futures" {})
                         :result
                         (filter #(includes? (:name %) "-PERP")))
        filtered (if (empty? markets)
                   (->> all-markets
                        (sort-by :volumeUsd24h)
                        reverse
                        (take (:num-markets config)))
                   (->> all-markets
                        (map #(update % :underlying keyword))
                        (filter #(markets (:underlying %)))))]
    (reduce
     (fn [acc {:keys [name volumeUsd24h]}]
       (assoc acc (keyword name) (* (/ volumeUsd24h minutes-in-day) (:est-bar-mins config))))
     {}
     filtered)))

(defn get-market-info
  "fetch spot market info, but map to perps to match market data"
  [markets]
  (let [all-markets (->> (ftx-us "/markets" {})
                         :result
                         (map #(update % :quoteCurrency keyword))
                         (filter #(= :USD (:quoteCurrency %))))
        filtered (if (empty? markets)
                   (->> all-markets
                        (sort-by :volumeUsd24h)
                        reverse
                        (take (:num-markets config)))
                   (->> all-markets
                        (filter #(markets (keyword (:baseCurrency %))))))]
    (reduce
     (fn [acc {:keys [baseCurrency] :as m}]
       ;; here is the hacky bit
       (assoc acc (keyword (str baseCurrency "-PERP")) m))
     {}
     filtered)))

(defn fname [market end-ts]
  (format "resources/%s_%s.npy" (name market) end-ts))

(defn freeze-trades [market end-ts data]
  (when (and (not (nil? data)) (seq data))
    (nippy/freeze-to-file (fname market end-ts) data)))

(defn thaw-trades [market end-ts]
  (try
    (nippy/thaw-from-file (fname market end-ts))
    (catch Exception _ nil)))

;; epochs here are in SECONDS
(defn fetch-historical-trades [market end-ts lookback-days]
  (let [path (format "/markets/%s/trades" (name market))
        limit 100 ;; current ftx max
        start-ts (long (- end-ts (* lookback-days 24 60 60)))
        params {:start_time start-ts
                :limit limit}
        unsorted
        (loop [results [] ids #{} next-end end-ts]
          (let [trades (->> (assoc params :end_time next-end)
                            (ftx path)
                            :result
                            (map #(update % :side keyword)))
                filtered (remove #(ids (:id %)) trades)
                updated (concat results filtered)]
            (if (< (count trades) limit)
              updated
              (recur updated
                     (union ids (set (map :id filtered)))
                     (reduce #(min %1 (long (/ (epoch (:time %2)) 1000))) next-end trades)))))
        sorted (sort-by :time unsorted)]
    (freeze-trades market end-ts sorted)
    sorted))

(defn historical-trades [market end-ts lookback-days]
  (let [trades (thaw-trades market end-ts)]
    (if (nil? trades)
      (fetch-historical-trades market end-ts lookback-days)
      trades)))

;; db stuff
(defn fetch
  [path params]
  (map
   (fn [x]
     (-> (update x :side #(if (= "buy" %) "b" "s"))
         (update :time instant)))
   (:result (ftx path params))))

(defn db-insert [conn data]
  (jdbc/execute!
   conn
   (-> (h/insert-into :trades)
       (h/values data)
       (sql/format {:pretty true}))))

(def db {:dbtype "postgres"
         :dbname "postgres"
         :user "postgres"
         :password "password"})

;; still need to specify in seconds
(defn fetch-to-db [market end-ts lookback-days]
  (let [path (format "/markets/%s/trades" (name market))
        limit 100
        start-ts (long (- end-ts (* lookback-days 24 60 60)))
        params {:start_time start-ts
                :limit limit}
        next-end (atom end-ts)
        last-count (atom limit)
        ds (jdbc/get-datasource db)]
    (with-open [conn (jdbc/get-connection ds)]
      (while (>= @last-count limit)
        (let [trades (fetch path (assoc params :end_time @next-end))]
          (swap! next-end (fn [x]
                            (reduce
                             #(min %1 (.getEpochSecond (:time %2)))
                             x
                             trades)))
          (swap! last-count (fn [_] (count trades)))
          (db-insert conn trades))))))

(defn get-db-trades []
  (let [ds (jdbc/get-datasource db)
        selected (jdbc/execute!
                  ds
                  (-> (h/select :*)
                      (h/from :trades)
                      (h/order-by [:trades.time :asc])
                      (sql/format {:pretty true})))]
    (map
     (fn [t]
       (-> (rename-keys t {:trades/time :time
                           :trades/id :id
                           :trades/price :price
                           :trades/size :size
                           :trades/side :side
                           :trades/liquidation :liquidation})
           (update :side #(if (= % "b") :buy :sell))))
     selected)))
