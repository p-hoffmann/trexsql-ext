(defproject org.trex/trexsql "0.1.21"
  :description "Clojure DuckDB library for TREX - replaces bao with JVM implementation"
  :url "https://github.com/p-hoffmann/trex-java"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}

  :repositories [["jitpack" "https://jitpack.io"]]

  ;; Global exclusions for logging - let the container provide these
  :exclusions [ch.qos.logback/logback-classic
               ch.qos.logback/logback-core
               org.slf4j/slf4j-api]

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.github.p-hoffmann/trexsql-java "v0.1.4"]
                 [org.clojure/tools.cli "1.1.230"]
                 [org.clojure/data.json "2.5.0"]
                 ;; HoneySQL for SQL generation (005-jdbc-batch-cache)
                 [com.github.seancorfield/honeysql "2.6.1196"]
                 ;; HikariCP for connection pooling (005-jdbc-batch-cache)
                 [com.zaxxer/HikariCP "5.1.0"]
                 ;; Logging (T4.1.1-T4.1.2) - provided by container
                 [org.clojure/tools.logging "1.2.4"]
                 ;; Ring for HTTP/Servlet integration
                 [ring/ring-core "1.14.1"]
                 [org.ring-clojure/ring-jakarta-servlet "1.14.1"]
                 [ring/ring-json "0.5.1"]
                 ;; Jakarta Servlet API (provided at runtime by container)
                 [jakarta.servlet/jakarta.servlet-api "6.0.0" :scope "provided"]
                 ;; Reitit for routing
                 [metosin/reitit-ring "0.7.2"]
                 [clj-http "3.12.3"]]

  :source-paths ["src"]
  :test-paths ["test"]
  :resource-paths ["resources"]

  :main trexsql.core

  :aot [trexsql.api trexsql.core trexsql.servlet]

  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]]}
             :uberjar {:aot :all
                       :uberjar-name "trexsql-%s-standalone.jar"}}

  :repl-options {:init-ns trexsql.core})
