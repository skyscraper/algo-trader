(defproject algo-trader "0.1.0-SNAPSHOT"
  :description "algo trader boilerplate"
  :url "http://github.com/skyscraper/algo-trader"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"
            :distribution :repo}
  :dependencies [[aleph "0.4.7-alpha7"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [cheshire "5.10.0"]
                 [clojure.java-time "0.3.2"]
                 [com.datadoghq/java-dogstatsd-client "2.11.0"]
                 [com.taoensso/nippy "3.1.1"]
                 [commons-codec/commons-codec "1.15"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/core.async "1.3.618"]
                 [org.clojure/data.csv "1.0.0"]
                 [org.clojure/tools.logging "1.1.0"]]
  :repl-options {:init-ns algo-trader.core}
  :main algo-trader.core
  :aot :all)
