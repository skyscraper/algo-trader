(ns algo-trader.db
  (:require [algo-trader.api :refer [fetch-for-db]]
            [algo-trader.config :refer [config]]
            [algo-trader.utils :refer [underlying-kw]]
            [clojure.set :refer [rename-keys]]
            [clojure.string :refer [lower-case]]
            [clojure.tools.logging :as log]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [java-time :refer [instant]]
            [next.jdbc :as jdbc]
            [next.jdbc.date-time]))

(def hypertable-template
  "SELECT create_hypertable('%1$s', 'time');")

(defn table-name [underlying]
  (keyword (lower-case (str (name underlying) "_trades"))))

(defn create-hypertable [conn underlying]
  (let [t-name (table-name underlying)]
    (jdbc/execute!
     conn
     (-> (h/create-table t-name :if-not-exists)
         (h/with-columns [[:time :timestamptz [:not nil]]
                          [:id :bigint]
                          [:price :double :precision]
                          [:size :double :precision]
                          [:side [:char 1]]
                          [:liquidation :boolean]])
         (sql/format {:pretty true})))
    (try
      (jdbc/execute!
       conn
       (-> [(format hypertable-template (name t-name))]))
      (catch Exception e
        (log/info (.getMessage e))))))

(defn get-ult-ts [market comp-kw]
  (try
    (let [tkw (table-name (underlying-kw market))
          tn (name tkw)
          t-time (keyword (str tn ".time"))
          ds (jdbc/get-datasource (:db config))
          stmt (-> (h/select t-time)
                   (h/from tkw)
                   (h/order-by [t-time comp-kw])
                   (h/limit 1)
                   (sql/format {:pretty true}))]
      (.getTime ((keyword tn "time") (first (jdbc/execute! ds stmt)))))
    (catch Exception _ nil)))

(defn get-first-ts [market]
  (get-ult-ts market :asc))

(defn get-last-ts [market]
  (get-ult-ts market :desc))

(defn db-insert [conn data underlying]
  (jdbc/execute!
   conn
   (-> (h/insert-into (table-name underlying))
       (h/values data)
       (sql/format {:pretty true}))))

(defn fetch-and-store [market lookback-days append?]
  (let [underlying (underlying-kw market)
        path (format "/markets/%s/trades" (name market))
        limit 100
        delta (* lookback-days 24 60 60)
        now (long (/ (System/currentTimeMillis) 1000))
        [start-ts end-ts] (if append?
                            (if-let [start (long (/ (get-last-ts market) 1000))]
                              [start (min now (+ start delta))]
                              [(- now delta) now])
                            (let [end (or (long (/ (get-first-ts market) 1000)) now)]
                              [(- end delta) end]))
        params {:start_time start-ts
                :limit limit}
        next-end (atom end-ts)
        last-count (atom limit)
        ds (jdbc/get-datasource (:db config))]
    (with-open [conn (jdbc/get-connection ds)]
      (create-hypertable conn underlying)
      (while (>= @last-count limit)
        (let [trades (fetch-for-db path (assoc params :end_time @next-end))
              ct (count trades)]
          (swap! next-end (fn [x]
                            (reduce
                             #(min %1 (.getEpochSecond (:time %2)))
                             x
                             trades)))
          (swap! last-count (fn [_] ct))
          (when (> ct 0)
            (db-insert conn trades underlying)))))))

(defn get-trades [market from-ts to-ts]
  (let [f-date (instant from-ts)
        t-date (instant to-ts)
        ds (jdbc/get-datasource (:db config))
        tkw (table-name (underlying-kw market))
        tn (name tkw)
        t-time (keyword (str tn ".time"))
        selected (jdbc/execute!
                  ds
                  (-> (h/select :*)
                      (h/from tkw)
                      (h/where :and
                               [:>= t-time f-date]
                               [:< t-time t-date])
                      (h/order-by [t-time :asc])
                      (sql/format {:pretty true})))]
    (map
     (fn [t]
       (-> (rename-keys t {(keyword tn "time") :time
                           (keyword tn "id") :id
                           (keyword tn "price") :price
                           (keyword tn "size") :size
                           (keyword tn "side") :side
                           (keyword tn "liquidation") :liquidation})
           (update :side #(if (= % "b") :buy :sell))))
     selected)))


