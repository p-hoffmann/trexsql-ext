(ns trexsql.circe
  "Circe JSON to SQL conversion and execution functionality.
   Uses DuckDB's circe extension for cohort SQL generation."
  (:require [trexsql.db :as db]
            [trexsql.util :as util]
            [trexsql.errors :as errors]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [java.util Map List ArrayList HashMap Base64]))

(defn encode-base64
  "Encode a string to base64 for circe JSON input.
   Uses UTF-8 encoding as required by circe."
  [^String s]
  (.encodeToString (Base64/getEncoder) (.getBytes s "UTF-8")))

(defn validate-circe-options
  "Validate CirceOptions. Returns nil if valid, error message if invalid."
  [options]
  (cond
    (nil? options)
    "Circe options cannot be nil"

    (str/blank? (:cdm-schema options))
    "Missing required option: cdm-schema"

    (some? (db/validate-identifier (:cdm-schema options)))
    (str "Invalid cdm-schema: " (db/validate-identifier (:cdm-schema options)))

    (str/blank? (:result-schema options))
    "Missing required option: result-schema"

    (some? (db/validate-identifier (:result-schema options)))
    (str "Invalid result-schema: " (db/validate-identifier (:result-schema options)))

    (nil? (:cohort-id options))
    "Missing required option: cohort-id"

    (not (integer? (:cohort-id options)))
    (str "Invalid cohort-id: " (:cohort-id options) ". Must be an integer.")

    (and (:cohort-id options)
         (< (:cohort-id options) 1))
    (str "Invalid cohort-id: " (:cohort-id options) ". Must be a positive integer.")

    :else nil))

(defn validate-circe-json
  "Validate that the input JSON is non-empty and properly formatted.
   Returns nil if valid, error message if invalid."
  [json-str]
  (cond
    (str/blank? json-str)
    "Circe JSON cannot be empty"

    (not (str/starts-with? (str/trim json-str) "{"))
    "Circe JSON must be a valid JSON object starting with {"

    :else nil))

(defn java-map->circe-options
  "Convert Java Map to circe options map with keyword keys.
   Applies defaults for optional fields."
  [^Map m]
  (when m
    (let [clj-map (util/java-map->clj-map m)]
      {:cdm-schema (:cdm-schema clj-map)
       :result-schema (:result-schema clj-map)
       :target-table (or (:target-table clj-map) "cohort")
       :cohort-id (:cohort-id clj-map)
       :generate-stats (boolean (or (:generate-stats clj-map) false))})))

(defn build-circe-options-json
  "Build JSON string for circe options from options map.
   Format expected by circe_json_to_sql function."
  [options]
  (db/validate-identifier! (:cdm-schema options) "cdm-schema")
  (db/validate-identifier! (:result-schema options) "result-schema")
  (db/validate-identifier! (:target-table options) "target-table")
  (format "{\"cdmSchema\":\"%s\",\"resultSchema\":\"%s\",\"targetTable\":\"%s\",\"cohortId\":%d,\"generateStats\":%s}"
          (util/escape-json-string (:cdm-schema options))
          (util/escape-json-string (:result-schema options))
          (util/escape-json-string (:target-table options))
          (:cohort-id options)
          (if (:generate-stats options) "true" "false")))

(defn check-circe-error
  "Check if circe output indicates an error.
   Circe returns errors as /* circe error: ... */
   Returns error message if found, nil otherwise."
  [sql-str]
  (when (and sql-str (str/starts-with? sql-str "/* circe error"))
    (let [end-idx (str/index-of sql-str "*/")]
      (if end-idx
        (subs sql-str 3 end-idx)
        sql-str))))

(defn render-circe-sql
  "Render SQL from Circe JSON using circe extension.
   Returns SQL string.
   Throws exception on circe error or extension failure."
  [db json-str options]
  (when-let [error (validate-circe-json json-str)]
    (throw (errors/validation-error error {:field "json"})))
  (when-let [error (validate-circe-options options)]
    (throw (errors/validation-error error {:field "options"})))
  (when-not (util/load-circe-extension! db)
    (throw (errors/extension-error "Failed to load circe extension" "circe")))
  (let [base64-json (encode-base64 json-str)
        options-json (build-circe-options-json options)
        sql (format "SELECT circe_sql_translate(circe_json_to_sql('%s', '%s'), 'duckdb') AS sql"
                    base64-json options-json)
        results (db/query db sql)]
    (if (empty? results)
      (throw (errors/sql-error "Circe returned no results" sql))
      (let [rendered-sql (.get ^HashMap (first results) "sql")]
        (when-let [error (check-circe-error rendered-sql)]
          (throw (errors/sql-error (str "Circe error: " error) sql)))
        rendered-sql))))

(defn execute-circe-sql
  "Execute rendered circe SQL and return rows affected.
   Returns number of rows affected."
  [db sql]
  (try
    (db/execute! db sql)
    true
    (catch Exception e
      (throw (errors/sql-error "Failed to execute circe SQL" sql e)))))

(defn execute-circe
  "Execute Circe JSON cohort definition.
   Returns result map with :success, :sql, :rows-affected, :error."
  [db json-str options]
  (try
    (let [sql (render-circe-sql db json-str options)]
      (execute-circe-sql db sql)
      {:success true
       :sql sql
       :rows-affected 0  ; TODO: count cohort rows if possible
       :error nil})
    (catch clojure.lang.ExceptionInfo e
      {:success false
       :sql (errors/error-sql e)
       :rows-affected 0
       :error (.getMessage e)
       :error-type (errors/error-type e)})
    (catch Exception e
      {:success false
       :sql nil
       :rows-affected 0
       :error (.getMessage e)
       :error-type :unknown})))

(defn circe-result->java-map
  "Convert circe result map to Java HashMap."
  [result]
  (doto (HashMap.)
    (.put "success" (boolean (:success result)))
    (.put "sql" (:sql result))
    (.put "rows-affected" (or (:rows-affected result) 0))
    (.put "error" (:error result))))

(defn render-circe-to-sql
  "Render Circe JSON to SQL without execution.
   Returns SQL string.
   Throws exception on error."
  [db json-str options]
  (render-circe-sql db json-str options))
