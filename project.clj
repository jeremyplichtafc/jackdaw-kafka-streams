(defproject clojure-kafka-streams "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.7.559"]
                 [fundingcircle/jackdaw "0.6.4"]
                 [willa "0.1.2"]
                 [org.slf4j/slf4j-api "1.7.30"]
                 [org.slf4j/slf4j-log4j12 "1.7.30"]
                 [clj-time "0.15.2"]]
  :main ^:skip-aot clojure-kafka-streams.core
  :resource-paths ["src/resources"]
  :target-path "target/%s"
  :profiles {
             :uberjar {:aot :all}
             :dev {:dependencies [[org.clojure/test.check "0.9.0"]]}
             })
