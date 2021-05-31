(ns algo-trader.db
  (:require [algo-trader.api :refer [fetch-for-db]]
            [clojure.set :refer [rename-keys]]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [java-time :refer [instant]]
            [next.jdbc :as jdbc]
            [next.jdbc.date-time]))

(def db {:dbtype "postgres"
         :dbname "postgres"
         :user "postgres"
         :password "password"})

(defn db-insert [conn data]
  (jdbc/execute!
   conn
   (-> (h/insert-into :trades)
       (h/values data)
       (sql/format {:pretty true}))))

(defn fetch-and-store [market end-ts lookback-days]
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
        (let [trades (fetch-for-db path (assoc params :end_time @next-end))]
          (swap! next-end (fn [x]
                            (reduce
                             #(min %1 (.getEpochSecond (:time %2)))
                             x
                             trades)))
          (swap! last-count (fn [_] (count trades)))
          (db-insert conn trades))))))

(defn get-trades [from-ts to-ts]
  (let [f-date (instant from-ts)
        t-date (instant to-ts)
        ds (jdbc/get-datasource db)
        selected (jdbc/execute!
                  ds
                  (-> (h/select :*)
                      (h/from :trades)
                      (h/where :and
                               [:>= :trades.time f-date]
                               [:< :trades.time t-date])
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

(defn get-ult-ts [comp-kw]
  (let [ds (jdbc/get-datasource db)
        stmt (-> (h/select :trades.time)
                 (h/from :trades)
                 (h/order-by [:trades.time comp-kw])
                 (h/limit 1)
                 (sql/format {:pretty true}))]
    (.getTime (:trades/time (first (jdbc/execute! ds stmt))))))

(defn get-first-ts []
  (get-ult-ts :asc))

(defn get-last-ts []
  (get-ult-ts :desc))
