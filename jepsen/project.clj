(defproject jepsen.switchboard "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Switchboard, a multi-master AMQP 0-9-1 broker."
  :url "https://example.invalid/switchboard"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.14"]
                 [com.rabbitmq/amqp-client "5.37.0"]
                 [org.clojure/tools.cli "1.4.256"]
                 ;; amqp-client wants slf4j 1.x and jepsen's unilog targets
                 ;; logback 1.2; force a consistent pair.
                 [org.slf4j/slf4j-api "1.7.36"]
                 [ch.qos.logback/logback-classic "1.2.13"]]
  :main jepsen.switchboard
  :jvm-opts ["-Xmx6g"
             "-server"
             "-Djava.awt.headless=true"]
  :profiles {:uberjar {:aot :all}})
