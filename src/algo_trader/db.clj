(ns algo-trader.db
  (:require [algo-trader.api :refer [fetch-for-db]]
            [algo-trader.utils :refer [underlying-kw]]
            [clojure.set :refer [rename-keys]]
            [clojure.string :refer [lower-case]]
            [environ.core :refer [env]]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [java-time :refer [instant]]
            [next.jdbc :as jdbc]
            [next.jdbc.date-time]))

;; temporary env var hack
(def db
  {:dbtype   (:app-dbtype env "postgres")
   :dbname   (:app-dbname env "x")
   :host     (:app-dbhost env "x")
   :user     (:app-dbuser env "x")
   :password (:app-dbpassword env "x")})

(defn table-name [underlying]
  (keyword (lower-case (str (name underlying) "_trades"))))

(defn create-table [conn underlying]
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
          (sql/format {:pretty true})))))

(defn get-ult-ts [underlying comp-kw]
  (try
    (let [tkw (table-name underlying)
          tn (name tkw)
          t-time (keyword (str tn ".time"))
          ds (jdbc/get-datasource db)
          stmt (-> (h/select t-time)
                   (h/from tkw)
                   (h/order-by [t-time comp-kw])
                   (h/limit 1)
                   (sql/format {:pretty true}))]
      (.getTime ((keyword tn "time") (first (jdbc/execute! ds stmt)))))
    (catch Exception _ nil)))

(defn get-first-ts [underlying]
  (get-ult-ts underlying :asc))

(defn get-last-ts [underlying]
  (get-ult-ts underlying :desc))

(defn db-insert [conn data underlying]
  (jdbc/execute!
    conn
    (-> (h/insert-into (table-name underlying))
        (h/values data)
        (sql/format {:pretty true}))))

(defn fetch-and-store [market lookback-days append?]
  (let [underlying (underlying-kw market)
        path (format "/api/markets/%s/trades" (name market))
        limit 100
        delta (* lookback-days 24 60 60)
        now (long (/ (System/currentTimeMillis) 1000))
        [start-ts end-ts] (if append?
                            (if-let [start (get-last-ts market)]
                              (let [start-s (long (/ start 1000))]
                                [start-s (min now (+ start-s delta))])
                              [(- now delta) now])
                            (if-let [end (get-first-ts market)]
                              (let [end-s (long (/ end 1000))]
                                [(- end-s delta) end-s])
                              [(- now delta) now]))
        params {:start_time start-ts
                :limit      limit}
        next-end (atom end-ts)
        last-count (atom limit)
        ds (jdbc/get-datasource db)]
    (with-open [conn (jdbc/get-connection ds)]
      (create-table conn underlying)
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

(defn get-trades [underlying from-ts to-ts]
  (let [f-date (instant from-ts)
        t-date (instant to-ts)
        ds (jdbc/get-datasource db)
        tkw (table-name underlying)
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
        (-> (rename-keys t {(keyword tn "time")        :time
                            (keyword tn "id")          :id
                            (keyword tn "price")       :price
                            (keyword tn "size")        :size
                            (keyword tn "side")        :side
                            (keyword tn "liquidation") :liquidation})
            (update :side #(if (= % "b") :buy :sell))
            (assoc :source :ftx)))
      selected)))

(def get-trades-memo (memoize get-trades))
