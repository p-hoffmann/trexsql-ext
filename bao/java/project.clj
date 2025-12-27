(defproject com.trex/trexsql "0.1.1"
  :description "Clojure DuckDB library for TREX - replaces bao with JVM implementation"
  :url "https://github.com/p-hoffmann/trex-java"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}

  :repositories [["jitpack" "https://jitpack.io"]]

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.github.p-hoffmann/trexsql-java "v0.1.1"]
                 [org.clojure/tools.cli "1.1.230"]
                 [org.clojure/data.json "2.5.0"]
                 ;; HoneySQL for SQL generation (005-jdbc-batch-cache)
                 [com.github.seancorfield/honeysql "2.6.1196"]
                 ;; HikariCP for connection pooling (005-jdbc-batch-cache)
                 [com.zaxxer/HikariCP "5.1.0"]
                 ;; Logging (T4.1.1-T4.1.2)
                 [org.clojure/tools.logging "1.2.4"]
                 [ch.qos.logback/logback-classic "1.4.11"]]

  :source-paths ["src"]
  :test-paths ["test"]
  :resource-paths ["resources"]

  :main trexsql.core

  :aot [trexsql.api trexsql.core]

  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]]}
             :uberjar {:aot :all
                       :uberjar-name "trexsql-%s-standalone.jar"}}

  :repl-options {:init-ns trexsql.core})
