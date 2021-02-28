(ns algo-trader.api
  (:require [aleph.http :as http]
            [algo-trader.config :refer [config trade-event quote-event]]
            [cheshire.core :refer [generate-string parse-string]]
            [clojure.string :refer [join upper-case]]
            [clojure.tools.logging :as log]
            [manifold.stream :as s]))

(defn polygon-websocket [cluster]
  (let [conn @(http/websocket-client cluster {:epoll? true})]
    (log/info (:message (first (parse-string @(s/take! conn) true))))
    (s/put! conn (generate-string {:action :auth :params (:polygon-api-key config)}))
    (log/info (:message (first (parse-string @(s/take! conn) true))))
    conn))

(defn symbol-str [syms]
  (->> syms
       (map #(format "%1$s.%3$s,%2$s.%3$s"
                     (name trade-event)
                     (name quote-event)
                     (upper-case (name %))))
       (join ",")))

(defn- polygon-subscription-manage [conn syms action]
  (->> (symbol-str syms)
       (assoc {:action action} :params)
       generate-string
       (s/put! conn)))

(defn polygon-subscribe [conn syms]
  (polygon-subscription-manage conn syms :subscribe))

(defn polygon-unsubscribe [conn syms]
  (polygon-subscription-manage conn syms :unsubscribe))
