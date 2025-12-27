(ns trexsql.webapi
  "WebAPI integration handlers."
  (:require [trexsql.datamart :as datamart]
            [trexsql.jobs :as jobs]
            [trexsql.vocab :as vocab]
            [trexsql.circe :as circe]
            [clojure.string :as str]
            [clojure.data.json :as json])
  (:import [java.util HashMap ArrayList Map]
           [java.io File]))

(defonce ^:private source-repository (atom nil))

(defn set-source-repository! [repo]
  (reset! source-repository repo))

(defn get-source-repository []
  @source-repository)

(defn- find-source-by-key [source-key]
  (when-let [repo @source-repository]
    (try
      (.findBySourceKey repo source-key)
      (catch Exception e
        nil))))

(defn- source->credentials [source]
  (when source
    (let [dialect (.getSourceDialect source)]
      {:dialect dialect
       :connection-string (.getSourceConnection source)
       :user (.getUsername source)
       :password (.getPassword source)
       :jdbc-url (when (datamart/jdbc-dialect? dialect)
                   (.getSourceConnection source))})))

(defn response
  ([status body]
   {:status status :body body})
  ([status body headers]
   {:status status :body body :headers headers}))

(defn ok [body]
  (response 200 body))

(defn created [body]
  (response 201 body))

(defn no-content []
  (response 204 nil))

(defn bad-request [message]
  (response 400 {:error "BAD_REQUEST" :message message}))

(defn not-found [message]
  (response 404 {:error "NOT_FOUND" :message message}))

(defn conflict [message]
  (response 409 {:error "CONFLICT" :message message}))

(defn internal-error [message]
  (response 500 {:error "INTERNAL_ERROR" :message message}))

(defn service-unavailable [message]
  (response 503 {:error "SERVICE_UNAVAILABLE" :message message}))

(defn- parse-json [s]
  (when (and s (not (str/blank? s)))
    (try
      (json/read-str s :key-fn keyword)
      (catch Exception e
        nil))))

(defn- java-map->clj [m]
  (cond
    (instance? Map m)
    (into {} (for [[k v] m]
               [(keyword k) (java-map->clj v)]))

    (instance? java.util.List m)
    (vec (map java-map->clj m))

    :else m))

(defn- validate-database-code
  "Validate database-code for safe use. Returns nil if valid, error message if invalid."
  [database-code]
  (cond
    (nil? database-code)
    "database-code is required"

    (not (string? database-code))
    "database-code must be a string"

    (str/blank? database-code)
    "database-code cannot be empty"

    (not (re-matches #"^[a-zA-Z0-9_-]+$" database-code))
    (str "Invalid database-code: " database-code
         ". Must contain only alphanumeric, underscore, or hyphen characters.")

    (> (count database-code) 128)
    "database-code is too long (max 128 characters)"

    :else nil))

(defn- build-credentials-map [credentials]
  (let [m (doto (HashMap.)
            (.put "dialect" (:dialect credentials))
            (.put "connection-string" (:connection-string credentials))
            (.put "user" (:user credentials))
            (.put "password" (:password credentials)))]
    (when (:jdbc-url credentials)
      (.put m "jdbc-url" (:jdbc-url credentials)))
    m))

(defn- build-datamart-config [database-code schema-name cache-path credentials request]
  (let [config (doto (HashMap.)
                 (.put "database-code" database-code)
                 (.put "schema-name" schema-name)
                 (.put "cache-path" cache-path)
                 (.put "source-credentials" (build-credentials-map credentials))
                 (.put "fts-tables" (ArrayList. (or (:ftsTables request) ["concept"]))))]
    (when-let [v (:targetSchemaName request)] (.put config "target-schema-name" v))
    (when-let [v (:tables request)] (.put config "tables" (ArrayList. v)))
    (when-let [v (:patientFilter request)] (.put config "patient-filter" (ArrayList. v)))
    (when-let [v (:timestampFilter request)] (.put config "timestamp-filter" v))
    config))

(defn- build-circe-options [cdm-schema result-schema cohort-id target-table generate-stats]
  (doto (HashMap.)
    (.put "cdm-schema" cdm-schema)
    (.put "result-schema" result-schema)
    (.put "cohort-id" cohort-id)
    (.put "target-table" (or target-table "cohort"))
    (.put "generate-stats" (boolean generate-stats))))

(defn- build-vocab-options [database-code max-rows]
  (doto (HashMap.)
    (.put "database-code" database-code)
    (.put "max-rows" max-rows)))

(defn- get-cache-path [cache-base-path database-code]
  (str cache-base-path "/" database-code ".db"))

(defn- handle-create-cache [db source-key body params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [request (parse-json body)]
        (if-not request
          (bad-request "Invalid JSON body")
          (if (str/blank? (:schemaName request))
            (bad-request "schemaName is required")
            (let [database-code (or (:databaseCode request) source-key)]
              (if-let [validation-error (validate-database-code database-code)]
                (bad-request validation-error)
                (try
                  (let [cache-path (or (:cachePath params) "./data/cache")
                        credentials (source->credentials source)
                        config-map (build-datamart-config database-code (:schemaName request)
                                                          cache-path credentials request)
                        validated-config (datamart/java-map->datamart-config config-map)]
                    (when-let [error (datamart/validate-config validated-config)]
                      (throw (IllegalArgumentException. error)))
                    (let [result (datamart/create-cache db validated-config nil)
                          java-result (datamart/result->java-map result)]
                      (ok (java-map->clj java-result))))
                  (catch IllegalArgumentException e
                    (bad-request (.getMessage e)))
                  (catch Exception e
                    (internal-error (.getMessage e))))))))))))

(defn- handle-get-cache-status [db source-key params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [database-code (or (:databaseCode params) source-key)]
        (if-let [validation-error (validate-database-code database-code)]
          (bad-request validation-error)
          (let [cache-path (or (:cachePath params) "./data/cache")
                cache-file (File. (get-cache-path cache-path database-code))
                exists? (.exists cache-file)
                attached? (try (datamart/is-attached? db database-code) (catch Exception _ false))
                job-status (try (jobs/get-job-status db database-code) (catch Exception _ nil))]
            (ok {:sourceKey source-key
                 :cacheExists exists?
                 :cacheAttached attached?
                 :cacheFilePath (when exists? (.getAbsolutePath cache-file))
                 :cacheSizeBytes (when exists? (.length cache-file))
                 :lastModified (when exists? (.lastModified cache-file))
                 :activeJob (when job-status
                              {:jobId (:job-execution-id job-status)
                               :status (:status job-status)
                               :startTime (str (:start-time job-status))
                               :endTime (when (:end-time job-status) (str (:end-time job-status)))
                               :progress {:totalTables (:total-tables job-status)
                                          :completedTables (:completed-tables job-status)
                                          :currentTable (:current-table job-status)
                                          :processedRows (:processed-rows job-status)}
                               :error (:error-message job-status)})})))))))

(defn- handle-list-cache-jobs [db params]
  "List all cache jobs, optionally filtered by status."
  (try
    (let [status (:status params)
          jobs (if status
                 (jobs/list-jobs db :status status)
                 (jobs/list-jobs db))]
      (ok {:jobs (mapv (fn [job]
                         {:databaseCode (:database-code job)
                          :sourceKey (:source-key job)
                          :status (:status job)
                          :startTime (str (:start-time job))
                          :endTime (when (:end-time job) (str (:end-time job)))
                          :totalTables (:total-tables job)
                          :completedTables (:completed-tables job)
                          :error (:error-message job)})
                       jobs)
           :count (count jobs)}))
    (catch Exception e
      (internal-error (.getMessage e)))))

(defn- handle-cancel-cache-job [db source-key params]
  "Cancel a running cache job."
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [database-code (or (:databaseCode params) source-key)
            job-status (try (jobs/get-job-status db database-code) (catch Exception _ nil))]
        (cond
          (nil? job-status)
          (not-found (str "No cache job found for: " database-code))

          (not= "RUNNING" (:status job-status))
          (conflict (str "Job is not running. Current status: " (:status job-status)))

          :else
          (do
            (jobs/update-local-status! db database-code "CANCELED")
            (ok {:databaseCode database-code
                 :status "CANCELED"
                 :message "Job cancellation requested"})))))))

(defn- handle-delete-cache [db source-key params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [database-code (or (:databaseCode params) source-key)
            cache-path (or (:cachePath params) "./data/cache")
            cache-file (File. (get-cache-path cache-path database-code))]
        (if-not (.exists cache-file)
          (not-found (str "Cache not found for source: " source-key))
          (do
            (when (try (datamart/is-attached? db database-code) (catch Exception _ false))
              (try (datamart/detach-database! db database-code) (catch Exception _)))
            (if (.delete cache-file)
              (no-content)
              (internal-error "Failed to delete cache file"))))))))

(defn- handle-execute-circe [db source-key body params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [request (parse-json body)]
        (if-not request
          (bad-request "Invalid JSON body")
          (let [{:keys [circeJson cdmSchema resultSchema cohortId targetTable generateStats]} request]
            (cond
              (str/blank? circeJson) (bad-request "circeJson is required")
              (str/blank? cdmSchema) (bad-request "cdmSchema is required")
              (str/blank? resultSchema) (bad-request "resultSchema is required")
              (nil? cohortId) (bad-request "cohortId is required")
              :else
              (try
                (let [options-map (build-circe-options cdmSchema resultSchema cohortId targetTable generateStats)
                      clj-options (circe/java-map->circe-options options-map)
                      result (circe/execute-circe db circeJson clj-options)
                      java-result (circe/circe-result->java-map result)]
                  (ok (java-map->clj java-result)))
                (catch Exception e
                  (internal-error (.getMessage e)))))))))))

(defn- handle-render-circe [db source-key body params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [request (parse-json body)]
        (if-not request
          (bad-request "Invalid JSON body")
          (let [{:keys [circeJson cdmSchema resultSchema cohortId targetTable generateStats]} request]
            (cond
              (str/blank? circeJson) (bad-request "circeJson is required")
              (str/blank? cdmSchema) (bad-request "cdmSchema is required")
              (str/blank? resultSchema) (bad-request "resultSchema is required")
              (nil? cohortId) (bad-request "cohortId is required")
              :else
              (try
                (let [options-map (build-circe-options cdmSchema resultSchema cohortId targetTable generateStats)
                      clj-options (circe/java-map->circe-options options-map)
                      sql (circe/render-circe-to-sql db circeJson clj-options)]
                  (ok {:success true :sql sql}))
                (catch Exception e
                  (internal-error (.getMessage e)))))))))))

(defn- handle-search-vocab [db source-key params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [query (:query params)
            max-rows (try (Integer/parseInt (or (:maxRows params) "1000")) (catch Exception _ 1000))
            database-code (or (:databaseCode params) source-key)]
        (if (str/blank? query)
          (bad-request "query parameter is required")
          (try
            (let [options-map (build-vocab-options database-code max-rows)
                  clj-options (vocab/java-map->search-options options-map)
                  results (vocab/search-vocab db query clj-options)
                  clj-results (mapv java-map->clj results)]
              (ok {:results clj-results :count (count clj-results)}))
            (catch Exception e
              (internal-error (.getMessage e)))))))))

(defn handle-request [db method path body headers params]
  (let [segments (vec (remove str/blank? (str/split (or path "") #"/")))
        source-key (first segments)
        resource (second segments)
        action (nth segments 2 nil)]
    (cond
      (str/blank? source-key)
      (bad-request "Source key is required in path")

      (and (= source-key "cache") (= resource "jobs"))
      (case method
        "GET" (handle-list-cache-jobs db params)
        (not-found (str "Unknown cache jobs endpoint: " method " /cache/jobs")))

      (= resource "cache")
      (case [method action]
        ["POST" nil] (handle-create-cache db source-key body params)
        ["GET" "status"] (handle-get-cache-status db source-key params)
        ["DELETE" nil] (handle-delete-cache db source-key params)
        ["DELETE" "job"] (handle-cancel-cache-job db source-key params)
        (not-found (str "Unknown cache endpoint: " method " /" (str/join "/" segments))))

      (= resource "circe")
      (case [method action]
        ["POST" "execute"] (handle-execute-circe db source-key body params)
        ["POST" "render"] (handle-render-circe db source-key body params)
        (not-found (str "Unknown circe endpoint: " method " /" (str/join "/" segments))))

      (= resource "vocab")
      (case [method action]
        ["GET" "search"] (handle-search-vocab db source-key params)
        (not-found (str "Unknown vocab endpoint: " method " /" (str/join "/" segments))))

      :else
      (not-found (str "Unknown resource: " resource)))))

(defn- clj->java [x]
  (cond
    (map? x)
    (let [m (HashMap.)]
      (doseq [[k v] x]
        (.put m (if (keyword? k) (name k) (str k)) (clj->java v)))
      m)

    (sequential? x)
    (let [l (ArrayList.)]
      (doseq [item x]
        (.add l (clj->java item)))
      l)

    (keyword? x)
    (name x)

    :else x))

(defn response->java-map [resp]
  (clj->java resp))
