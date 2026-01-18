(ns trexsql.servlet
  "Jakarta Servlet adapter for trexsql WebAPI.
   Exposes TrexServlet class that can be registered in Spring Boot."
  (:require [trexsql.webapi :as webapi]
            [trexsql.extensions :as ext]
            [trexsql.config :as config]
            [ring.util.jakarta.servlet :as servlet]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [jakarta.servlet.http HttpServletRequest HttpServletResponse])
  (:gen-class
   :name org.trex.TrexServlet
   :extends jakarta.servlet.http.HttpServlet
   :state state
   :init init-state
   :methods [[initTrex [Object Object] void]
             [initTrex [Object Object java.util.Map] void]]))

;; State: {:db atom, :source-repo atom, :handler atom, :config atom}
(defn -init-state
  "Initialize servlet state with atoms for db, source-repo, handler, and config."
  []
  [[] {:db (atom nil)
       :source-repo (atom nil)
       :handler (atom nil)
       :config (atom {})}])

(defn- wrap-strip-context
  "Middleware to strip /WebAPI/trexsql prefix from URI for Reitit routing."
  [handler]
  (fn [request]
    (let [uri (:uri request)
          stripped (str/replace uri #"^/WebAPI/trexsql" "")]
      (handler (assoc request :uri (if (str/blank? stripped) "/" stripped))))))

(defn- wrap-db
  "Middleware to inject db and config into request."
  [handler db-atom config-atom]
  (fn [request]
    (handler (assoc request
                    :db @db-atom
                    :trex-config @config-atom))))

(defn- wrap-exception-handler
  "Middleware to catch exceptions and return proper JSON error responses."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error e "Error handling request")
        {:status 500
         :headers {"Content-Type" "application/json"}
         :body {:error "INTERNAL_ERROR"
                :message (.getMessage e)}}))))

(defn- wrap-reitit-params
  "Middleware to copy :params to :query-params and :body-params for Reitit handlers."
  [handler]
  (fn [request]
    (let [params (:params request)
          body-params (:body request)]
      (handler (assoc request
                      :query-params params
                      :body-params (if (map? body-params) body-params {}))))))

(defn- create-app
  "Create Ring app with middleware stack."
  [db-atom config-atom]
  (-> (webapi/create-router)
      (wrap-db db-atom config-atom)
      wrap-exception-handler
      wrap-reitit-params
      wrap-keyword-params
      wrap-params
      (wrap-json-body {:keywords? true})
      wrap-json-response
      wrap-strip-context))

(defn- parse-trex-init []
  (when-let [init-json (System/getenv "TREX_INIT")]
    (try
      (let [parsed (json/read-str init-json :key-fn keyword)]
        (merge config/default-config parsed))
      (catch Exception e
        (log/warn e "Failed to parse TREX_INIT JSON, using defaults")
        config/default-config))))

(defn- run-init [db]
  (when-let [init-config (parse-trex-init)]
    (log/info (str "TREX_INIT config: " (pr-str init-config)))
    (try
      (let [conn (:connection db)]
        (try
          (with-open [stmt (.createStatement conn)]
            (.execute stmt "CALL disable_logging()"))
          (catch java.sql.SQLException _))
        (let [extensions-path (config/get-extensions-path init-config)
              loaded (ext/load-extensions conn extensions-path)]
          (reset! (:extensions-loaded db) loaded)
          (log/info (str "Loaded extensions: " (pr-str loaded)))))
      (catch Exception e
        (log/warn e "Failed to run init")))))

(defn -initTrex
  ([this db source-repo]
   (-initTrex this db source-repo nil))
  ([this db source-repo config]
   (log/info "Initializing TrexServlet with DuckDB instance")
   (let [state (.state this)
         config-map (when config
                      {:cache-path (get config "cache-path")})]
     (run-init db)
     (reset! (:db state) db)
     (reset! (:source-repo state) source-repo)
     (reset! (:config state) (or config-map {}))
     (webapi/set-source-repository! source-repo)
     (webapi/set-config! config-map)
     (reset! (:handler state) (create-app (:db state) (:config state)))
     (log/info "TrexServlet initialized successfully"))))

(defn -service
  "Handle HTTP request via Ring adapter.
   Delegates to the pre-created Ring handler."
  [this ^HttpServletRequest request ^HttpServletResponse response]
  (let [handler @(:handler (.state this))]
    (if handler
      (let [request-map (servlet/build-request-map request)
            response-map (handler request-map)]
        (servlet/update-servlet-response response response-map))
      (do
        (.setStatus response 503)
        (.setContentType response "application/json")
        (.write (.getWriter response) "{\"error\":\"SERVICE_UNAVAILABLE\",\"message\":\"Servlet not initialized\"}")))))
