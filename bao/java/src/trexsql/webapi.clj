(ns trexsql.webapi
  "WebAPI integration handlers with Reitit routing."
  (:require [trexsql.datamart :as datamart]
            [trexsql.jobs :as jobs]
            [trexsql.vocab :as vocab]
            [trexsql.circe :as circe]
            [trexsql.db :as db]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [reitit.ring :as ring])
  (:import [java.util HashMap ArrayList Map]
           [java.io File]))

(defonce ^:private source-repository (atom nil))
(defonce ^:private global-config (atom {:cache-path "./data/cache"}))

(defn set-source-repository! [repo]
  (reset! source-repository repo))

(defn set-config! [config]
  (when config
    (swap! global-config merge config)))

(defn get-cache-path-from-config []
  (:cache-path @global-config))

(defn get-source-repository []
  @source-repository)

(defn- find-source-by-key [source-key]
  (when-let [repo @source-repository]
    (try
      (.findBySourceKey repo source-key)
      (catch Exception e
        nil))))

(defn- parse-jdbc-url
  "Parse PostgreSQL JDBC URL to extract connection components.
   Format: jdbc:postgresql://host:port/database?user=xxx&password=xxx"
  [jdbc-url]
  (when jdbc-url
    (try
      (let [;; Remove jdbc:postgresql:// prefix
            url-part (str/replace jdbc-url #"^jdbc:postgresql://" "")
            ;; Split host:port/database?params
            [host-port-db params-str] (str/split url-part #"\?" 2)
            [host-port db] (str/split host-port-db #"/" 2)
            [host port-str] (str/split host-port #":" 2)
            port (when port-str (try (Integer/parseInt port-str) (catch Exception _ 5432)))
            ;; Parse params
            params (when params-str
                     (into {}
                       (for [param (str/split params-str #"&")]
                         (let [[k v] (str/split param #"=" 2)]
                           [(keyword k) v]))))]
        {:host host
         :port (or port 5432)
         :database-name db
         :user (:user params)
         :password (:password params)})
      (catch Exception _ nil))))

(defn- source->credentials [source]
  (when source
    (let [dialect (.getSourceDialect source)
          conn-str (.getSourceConnection source)
          explicit-user (.getUsername source)
          explicit-pass (.getPassword source)]
      (if (= "postgres" dialect)
        ;; For PostgreSQL, parse JDBC URL to get host/port/database
        (let [parsed (parse-jdbc-url conn-str)]
          (merge parsed
                 {:dialect dialect
                  :connection-string conn-str
                  ;; Prefer explicit user/password over URL params
                  :user (or explicit-user (:user parsed))
                  :password (or explicit-pass (:password parsed))}))
        ;; For other dialects
        {:dialect dialect
         :connection-string conn-str
         :user explicit-user
         :password explicit-pass
         :jdbc-url (when (datamart/jdbc-dialect? dialect) conn-str)}))))

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
    ;; Add PostgreSQL-specific fields
    (when (:host credentials) (.put m "host" (:host credentials)))
    (when (:port credentials) (.put m "port" (:port credentials)))
    (when (:database-name credentials) (.put m "database-name" (:database-name credentials)))
    ;; Add JDBC URL for JDBC dialects
    (when (:jdbc-url credentials) (.put m "jdbc-url" (:jdbc-url credentials)))
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
                  (let [cache-path (or (:cachePath params) (get-cache-path-from-config))
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
          (let [cache-path (or (:cachePath params) (get-cache-path-from-config))
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
            cache-path (or (:cachePath params) (get-cache-path-from-config))
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

;; HTTP handlers

(defn- list-cache-jobs-handler
  "Ring handler for GET /cache/jobs"
  [{:keys [db query-params]}]
  (handle-list-cache-jobs db query-params))

(defn- create-cache-handler
  "Ring handler for POST /:source-key/cache"
  [{:keys [db path-params body-params query-params]}]
  (let [source-key (:source-key path-params)]
    ;; body-params is already parsed by wrap-json-body middleware
    (let [source (find-source-by-key source-key)]
      (if-not source
        (not-found (str "Source not found: " source-key))
        (if (str/blank? (:schemaName body-params))
          (bad-request "schemaName is required")
          (let [database-code (or (:databaseCode body-params) source-key)]
            (if-let [validation-error (validate-database-code database-code)]
              (bad-request validation-error)
              (try
                (let [cache-path (or (:cachePath query-params) (get-cache-path-from-config))
                      credentials (source->credentials source)
                      config-map (build-datamart-config database-code (:schemaName body-params)
                                                        cache-path credentials body-params)
                      validated-config (datamart/java-map->datamart-config config-map)]
                  (when-let [error (datamart/validate-config validated-config)]
                    (throw (IllegalArgumentException. error)))
                  (let [result (datamart/create-cache db validated-config nil)
                        java-result (datamart/result->java-map result)]
                    (ok (java-map->clj java-result))))
                (catch IllegalArgumentException e
                  (bad-request (.getMessage e)))
                (catch Exception e
                  (internal-error (.getMessage e)))))))))))

(defn- get-cache-status-handler
  "Ring handler for GET /:source-key/cache/status"
  [{:keys [db path-params query-params]}]
  (let [source-key (:source-key path-params)]
    (handle-get-cache-status db source-key query-params)))

(defn- delete-cache-handler
  "Ring handler for DELETE /:source-key/cache"
  [{:keys [db path-params query-params]}]
  (let [source-key (:source-key path-params)]
    (handle-delete-cache db source-key query-params)))

(defn- cancel-cache-job-handler
  "Ring handler for DELETE /:source-key/cache/job"
  [{:keys [db path-params query-params]}]
  (let [source-key (:source-key path-params)]
    (handle-cancel-cache-job db source-key query-params)))

(defn- execute-circe-handler
  "Ring handler for POST /:source-key/circe/execute"
  [{:keys [db path-params body-params trex-config]}]
  (let [source-key (:source-key path-params)
        source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [{:keys [circeJson cdmSchema resultSchema cohortId targetTable generateStats]} body-params
            cache-path (or (:cache-path trex-config) (get-cache-path-from-config))]
        (cond
          (str/blank? circeJson) (bad-request "circeJson is required")
          (str/blank? cdmSchema) (bad-request "cdmSchema is required")
          (str/blank? resultSchema) (bad-request "resultSchema is required")
          (nil? cohortId) (bad-request "cohortId is required")
          :else
          (try
            ;; Attach cache for this source if not already attached
            (when-not (db/is-attached? db source-key)
              (log/info (format "Attaching cache for %s from %s" source-key cache-path))
              (db/attach-cache-file! db source-key cache-path))
            ;; Qualify schema names and target table with source-key (database alias)
            (let [qualified-cdm (str source-key "." cdmSchema)
                  qualified-results (str source-key "." resultSchema)
                  qualified-target (str qualified-results "." (or targetTable "cohort"))
                  options-map (build-circe-options qualified-cdm qualified-results cohortId qualified-target generateStats)
                  clj-options (circe/java-map->circe-options options-map)
                  result (circe/execute-circe db circeJson clj-options)
                  java-result (circe/circe-result->java-map result)]
              (ok (java-map->clj java-result)))
            (catch Exception e
              (internal-error (.getMessage e)))))))))

(defn- render-circe-handler
  "Ring handler for POST /:source-key/circe/render"
  [{:keys [db path-params body-params]}]
  (let [source-key (:source-key path-params)
        source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [{:keys [circeJson cdmSchema resultSchema cohortId targetTable generateStats]} body-params]
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
              (internal-error (.getMessage e)))))))))

(defn- search-vocab-handler
  "Ring handler for GET /:source-key/vocab/search"
  [{:keys [db path-params query-params]}]
  (let [source-key (:source-key path-params)]
    (handle-search-vocab db source-key query-params)))

;; Router

(def routes
  "WebAPI routes."
  [["/cache/jobs" {:get {:handler list-cache-jobs-handler}}]
   ["/:source-key"
    ["/cache" {:post {:handler create-cache-handler}
               :delete {:handler delete-cache-handler}}]
    ["/cache/status" {:get {:handler get-cache-status-handler}}]
    ["/cache/job" {:delete {:handler cancel-cache-job-handler}}]
    ["/circe/execute" {:post {:handler execute-circe-handler}}]
    ["/circe/render" {:post {:handler render-circe-handler}}]
    ["/vocab/search" {:get {:handler search-vocab-handler}}]]])

(defn create-router
  "Create Reitit Ring router for WebAPI endpoints.
   Returns a Ring handler function."
  []
  (ring/ring-handler
    (ring/router routes)
    (ring/create-default-handler
      {:not-found (constantly (not-found "Endpoint not found"))
       :method-not-allowed (constantly (response 405 {:error "METHOD_NOT_ALLOWED"
                                                       :message "Method not allowed"}))})))
