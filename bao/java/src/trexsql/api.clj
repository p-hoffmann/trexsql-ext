(ns trexsql.api
  "Java API for Trexsql via gen-class."
  (:require [trexsql.core :as core]
            [trexsql.db :as db]
            [trexsql.extensions :as ext]
            [trexsql.datamart :as datamart]
            [trexsql.vocab :as vocab]
            [trexsql.circe :as circe]
            [trexsql.http :as http]
            [trexsql.webapi :as webapi]
            [clojure.string :as str])
  (:import [java.util Map List ArrayList HashMap])
  (:gen-class
   :name com.trex.Trexsql
   :methods [^:static [init [java.util.Map] Object]
             ^:static [initWithServers [java.util.Map] Object]
             ^:static [query [Object String] java.util.List]
             ^:static [execute [Object String] boolean]
             ^:static [shutdown [Object] void]
             ^:static [isRunning [Object] boolean]
             ^:static [getLoadedExtensions [Object] java.util.List]
             ^:static [createDatamart [Object java.util.Map] java.util.Map]
             ^:static [createCache [Object java.util.Map java.util.function.Consumer] java.util.Map]
             ^:static [isAttached [Object String] boolean]
             ^:static [detachDatabase [Object String] void]
             ^:static [searchVocab [Object String java.util.Map] java.util.List]
             ^:static [executeCirce [Object String java.util.Map] java.util.Map]
             ^:static [renderCirceToSql [Object String java.util.Map] String]
             ^:static [httpRequest [Object String String java.util.Map String java.util.Map] java.util.Map]
             ^:static [setSourceRepository [Object] void]
             ^:static [handleRequest [Object String String String java.util.Map java.util.Map] java.util.Map]]))

(defn- java-map->clj-map
  [^Map m]
  (when m
    (into {} (for [[k v] m] [(if (string? k) (keyword k) k) v]))))

(defn- ensure-arraylist [coll]
  (if (instance? ArrayList coll) coll (ArrayList. ^java.util.Collection coll)))

(defn -init
  "Initialize DuckDB with extensions. Config: extensions-path."
  [^Map config]
  (core/init (java-map->clj-map config)))

(defn -initWithServers
  "Initialize DuckDB and start servers. Config: extensions-path, trexas-*, pgwire-*."
  [^Map config]
  (core/init-with-servers (java-map->clj-map config)))

(defn -query
  "Execute SQL query. Returns ArrayList<HashMap>."
  [db ^String sql]
  (ensure-arraylist (db/query db sql)))

(defn -execute
  "Execute non-query SQL. Returns true on success."
  [db ^String sql]
  (db/execute! db sql))

(defn -shutdown
  "Shutdown database and servers."
  [db]
  (core/shutdown! db))

(defn -isRunning
  "Check if servers are running."
  [db]
  (core/is-running? db))

(defn -getLoadedExtensions
  "List loaded extensions."
  [db]
  (ArrayList. ^java.util.Collection (vec (ext/loaded-extensions db))))

(defn -createDatamart
  "Create datamart cache from source database. Config: database-code, schema-name, source-credentials."
  [db ^Map config]
  (let [clj-config (datamart/java-map->datamart-config config)]
    (when-let [error (datamart/validate-config clj-config)]
      (throw (IllegalArgumentException. error)))
    (datamart/result->java-map (datamart/create-datamart db clj-config))))

(defn -createCache
  "Unified cache creation - routes automatically based on dialect.
   Config: database-code, schema-name, source-credentials (with jdbc-url for JDBC dialects).
   Progress callback receives Map with progress events during JDBC transfer.
   For PostgreSQL/BigQuery: uses native DuckDB scanner extensions.
   For SQL Server/Oracle/MySQL/MariaDB: uses JDBC batch transfer."
  [db ^Map config ^java.util.function.Consumer progress-callback]
  (let [clj-config (datamart/java-map->datamart-config config)
        ;; Wrap Java Consumer as Clojure function
        progress-fn (when progress-callback
                      (fn [event]
                        (let [java-event (doto (HashMap.)
                                           (.put "phase" (name (:phase event)))
                                           (.put "table" (:table event))
                                           (.put "tableIndex" (:table-index event))
                                           (.put "totalTables" (:total-tables event))
                                           (.put "rowsProcessed" (:rows-processed event))
                                           (.put "estimatedRows" (:estimated-rows event))
                                           (.put "rowsCopied" (:rows-copied event))
                                           (.put "durationMs" (:duration-ms event))
                                           (.put "error" (:error event)))]
                          (.accept progress-callback java-event))))]
    (when-let [error (datamart/validate-config clj-config)]
      (throw (IllegalArgumentException. error)))
    (datamart/result->java-map (datamart/create-cache db clj-config progress-fn))))

(defn -isAttached
  "Check if database is attached."
  [db ^String database-code]
  (datamart/is-attached? db database-code))

(defn -detachDatabase
  "Detach database."
  [db ^String database-code]
  (datamart/detach-database! db database-code))

(defn -searchVocab
  "Search vocabulary concepts. Options: database-code, schema-name, max-rows."
  [db ^String search-term ^Map options]
  (vocab/results->concept-list
   (vocab/search-vocab db search-term (vocab/java-map->search-options options))))

(defn -executeCirce
  "Execute Circe cohort definition. Options: cdm-schema, result-schema, cohort-id."
  [db ^String circe-json ^Map options]
  (circe/circe-result->java-map
   (circe/execute-circe db circe-json (circe/java-map->circe-options options))))

(defn -renderCirceToSql
  "Render Circe to SQL without execution."
  [db ^String circe-json ^Map options]
  (circe/render-circe-to-sql db circe-json (circe/java-map->circe-options options)))

(defn -httpRequest
  "Execute HTTP request to user worker. Options: timeout-ms, follow-redirects, max-redirects."
  [db ^String method ^String url ^Map headers ^String body ^Map options]
  (let [clj-headers (when headers (into {} headers))
        clj-options (when options
                      (cond-> {}
                        (.get options "timeout-ms") (assoc :timeout-ms (.get options "timeout-ms"))
                        (some? (.get options "follow-redirects")) (assoc :follow-redirects (.get options "follow-redirects"))
                        (.get options "max-redirects") (assoc :max-redirects (.get options "max-redirects"))))]
    (http/http-response->java-map
     (http/http-request db method url :headers clj-headers :body body :options clj-options))))

(defn -setSourceRepository
  "Set SourceRepository for WebAPI integration."
  [repo]
  (webapi/set-source-repository! repo))

(defn -handleRequest
  "Handle WebAPI request. Returns map with :status, :body, :headers."
  [db ^String method ^String path ^String body ^Map headers ^Map query-params]
  (webapi/response->java-map
   (webapi/handle-request db method path body
                          (java-map->clj-map headers)
                          (java-map->clj-map query-params))))
