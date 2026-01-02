(ns trexsql.servlet
  "Jakarta Servlet adapter for trexsql WebAPI.
   Exposes TrexServlet class that can be registered in Spring Boot."
  (:require [trexsql.webapi :as webapi]
            [ring.util.jakarta.servlet :as servlet]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [jakarta.servlet.http HttpServletRequest HttpServletResponse])
  (:gen-class
   :name com.trex.TrexServlet
   :extends jakarta.servlet.http.HttpServlet
   :state state
   :init init-state
   :methods [[initTrex [Object Object] void]]))

;; State: {:db atom, :source-repo atom, :handler atom}
(defn -init-state
  "Initialize servlet state with atoms for db, source-repo, and handler."
  []
  [[] {:db (atom nil)
       :source-repo (atom nil)
       :handler (atom nil)}])

(defn- wrap-strip-context
  "Middleware to strip /WebAPI/trexsql prefix from URI for Reitit routing."
  [handler]
  (fn [request]
    (let [uri (:uri request)
          stripped (str/replace uri #"^/WebAPI/trexsql" "")]
      (handler (assoc request :uri (if (str/blank? stripped) "/" stripped))))))

(defn- wrap-db
  "Middleware to inject db into request."
  [handler db-atom]
  (fn [request]
    (handler (assoc request :db @db-atom))))

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

(defn- create-app
  "Create Ring app with middleware stack."
  [db-atom]
  (-> (webapi/create-router)
      (wrap-db db-atom)
      wrap-exception-handler
      wrap-keyword-params
      wrap-params
      (wrap-json-body {:keywords? true})
      wrap-json-response
      wrap-strip-context))

(defn -initTrex
  "Initialize servlet with DuckDB instance and SourceRepository.
   Called from Spring Boot during servlet registration."
  [this db source-repo]
  (log/info "Initializing TrexServlet with DuckDB instance")
  (let [state (.state this)]
    (reset! (:db state) db)
    (reset! (:source-repo state) source-repo)
    (webapi/set-source-repository! source-repo)
    ;; Pre-create handler for performance
    (reset! (:handler state) (create-app (:db state)))
    (log/info "TrexServlet initialized successfully")))

(defn -service
  "Handle HTTP request via Ring adapter.
   Delegates to the pre-created Ring handler."
  [this ^HttpServletRequest request ^HttpServletResponse response]
  (let [handler @(:handler (.state this))]
    (if handler
      ((servlet/make-service-method handler) request response)
      (do
        (.setStatus response 503)
        (.getWriter response)
        (.write (.getWriter response) "{\"error\":\"SERVICE_UNAVAILABLE\",\"message\":\"Servlet not initialized\"}")))))
