(defproject algo-trader "0.1.0-SNAPSHOT"
  :description "algo trader boilerplate"
  :url "http://github.com/skyscraper/algo-trader"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"
            :distribution :repo}
  :dependencies [[aleph "0.4.7-alpha10"]
                 [clojure.java-time "0.3.3"]
                 [com.datadoghq/java-dogstatsd-client "4.0.0"]
                 [com.github.seancorfield/honeysql "2.2.840"]
                 [com.github.seancorfield/next.jdbc "1.2.761"]
                 [com.taoensso/timbre "5.1.2"]
                 [commons-codec/commons-codec "1.15"]
                 [environ "1.2.0"]
                 [metosin/jsonista "0.3.5"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/core.async "1.5.648"]
                 [org.clojure/data.csv "1.0.0"]
                 [org.postgresql/postgresql "42.3.1"]
                 [org.clojure/tools.cli "1.0.206"]]
  :repl-options {:init-ns algo-trader.core}
  :main algo-trader.core
  :aot :all)
