(ns trexsql.db
  "DuckDB connection management and query execution."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [trexsql.errors :as errors])
  (:import [java.sql Connection DriverManager ResultSet ResultSetMetaData SQLException]
           [java.util Properties ArrayList HashMap]))

(def ^:private identifier-pattern
  "Valid SQL identifier pattern: starts with letter/underscore,
   contains only alphanumeric and underscores, max 128 chars."
  #"^[a-zA-Z_][a-zA-Z0-9_]{0,127}$")

(defn validate-identifier
  "Validate that a string is a safe SQL identifier.
   Returns nil if valid, error message if invalid.
   Valid identifiers: start with letter/underscore, contain only alphanumeric/underscore."
  [^String s]
  (cond
    (nil? s)
    "Identifier cannot be nil"

    (str/blank? s)
    "Identifier cannot be empty"

    (> (count s) 128)
    (str "Identifier too long (max 128 chars): " (subs s 0 32) "...")

    (not (re-matches identifier-pattern s))
    (str "Invalid identifier '" s "'. Must start with letter/underscore and contain only alphanumeric/underscore characters.")

    :else nil))

(defn validate-identifier!
  "Validate identifier and throw validation error if invalid."
  [^String s ^String context]
  (when-let [error (validate-identifier s)]
    (throw (errors/validation-error (str context ": " error) {:field context}))))

(defn escape-identifier
  "Escape a SQL identifier by wrapping in double quotes and escaping internal quotes.
   Validates the identifier first to prevent SQL injection.
   Throws IllegalArgumentException if identifier is invalid."
  [^String s ^String context]
  (validate-identifier! s context)
  (str "\"" (str/replace s "\"" "\"\"") "\""))

(defn create-connection
  "Create a DuckDB connection with unsigned extensions enabled.
   Returns a java.sql.Connection to an in-memory database."
  []
  (let [props (doto (Properties.)
                (.setProperty "allow_unsigned_extensions" "true"))]
    (DriverManager/getConnection "jdbc:trex:" props)))

(defrecord TrexsqlDatabase [^Connection connection
                            extensions-loaded  ; atom of set
                            config
                            servers-running?   ; atom of boolean
                            closed?])          ; atom of boolean

(defn make-database
  "Create a new TrexsqlDatabase record.
   Uses atoms for mutable state (closed?, extensions-loaded, servers-running?)."
  [conn config]
  (map->TrexsqlDatabase {:connection conn
                         :extensions-loaded (atom #{})
                         :config config
                         :servers-running? (atom false)
                         :closed? (atom false)}))

(defn ensure-open!
  "Throw resource error if database is closed."
  [^TrexsqlDatabase db]
  (when @(:closed? db)
    (throw (errors/resource-error "Database is closed" :connection))))

(defn close!
  "Close the database connection and mark as closed."
  [^TrexsqlDatabase db]
  (when-not @(:closed? db)
    (when-let [conn (:connection db)]
      (.close conn))
    (reset! (:closed? db) true)))

(defn closed?
  "Check if the database connection is closed."
  [^TrexsqlDatabase db]
  @(:closed? db))

(defn extension-loaded?
  "Check if an extension has been loaded in this database session."
  [^TrexsqlDatabase db ^String ext-name]
  (contains? @(:extensions-loaded db) ext-name))

(defn load-extension!
  "Load a DuckDB extension if not already loaded.
   Caches loaded extensions to avoid redundant INSTALL/LOAD calls.
   Returns true if extension was loaded, false if already loaded."
  [^TrexsqlDatabase db ^String ext-name & {:keys [source] :or {source nil}}]
  (ensure-open! db)
  (if (extension-loaded? db ext-name)
    false
    (do
      (let [install-sql (if source
                          (format "INSTALL %s FROM %s" ext-name source)
                          (format "INSTALL %s" ext-name))]
        (try
          (with-open [stmt (.createStatement (:connection db))]
            (.execute stmt install-sql))
          (catch SQLException e
            (log/debug (format "Extension %s install returned: %s (may already be installed)"
                               ext-name (.getMessage e)))))
        (with-open [stmt (.createStatement (:connection db))]
          (.execute stmt (format "LOAD %s" ext-name))))
      (swap! (:extensions-loaded db) conj ext-name)
      true)))

(defn- resultset->row
  "Convert current ResultSet row to a HashMap."
  [^ResultSet rs ^ResultSetMetaData meta col-count]
  (let [row (HashMap.)]
    (doseq [i (range 1 (inc col-count))]
      (let [col-name (.getColumnLabel meta i)
            value (.getObject rs i)]
        (.put row col-name value)))
    row))

(defn result-set->list
  "Convert a ResultSet to an ArrayList of HashMaps.
   Each row is a HashMap<String, Object> with column names as keys."
  [^ResultSet rs]
  (let [meta (.getMetaData rs)
        col-count (.getColumnCount meta)
        results (ArrayList.)]
    (while (.next rs)
      (.add results (resultset->row rs meta col-count)))
    results))

(defn query
  "Execute a SQL query and return results as ArrayList<HashMap>.
   Throws sql-error on SQLException.
   Throws resource-error if database is closed."
  [^TrexsqlDatabase db ^String sql]
  (ensure-open! db)
  (try
    (with-open [stmt (.createStatement (:connection db))
                rs (.executeQuery stmt sql)]
      (result-set->list rs))
    (catch SQLException e
      (throw (errors/sql-error (str "SQL error: " (.getMessage e)) sql e)))))

(defn execute!
  "Execute a non-query SQL statement (DDL, DML, LOAD, etc.).
   Returns true on success.
   Throws sql-error on SQLException.
   Throws resource-error if database is closed."
  [^TrexsqlDatabase db ^String sql]
  (ensure-open! db)
  (try
    (with-open [stmt (.createStatement (:connection db))]
      (.execute stmt sql)
      true)
    (catch SQLException e
      (throw (errors/sql-error (str "SQL error: " (.getMessage e)) sql e)))))

(defn attach-cache-file!
  "Attach a DuckDB cache file using ATTACH IF NOT EXISTS.
   Creates the cache directory if it doesn't exist.
   Returns the database alias on success.
   Throws IllegalArgumentException if database-code is invalid.
   Throws RuntimeException if the cache file is locked or on other errors."
  [^TrexsqlDatabase db ^String database-code ^String cache-path]
  (ensure-open! db)
  (let [escaped-alias (escape-identifier database-code "database-code")
        cache-dir (java.io.File. cache-path)
        cache-file (java.io.File. cache-dir (str database-code ".db"))
        file-path (.getAbsolutePath cache-file)
        escaped-path (str/replace file-path "'" "''")]
    (when-not (.exists cache-dir)
      (.mkdirs cache-dir))
    (try
      (execute! db (format "ATTACH IF NOT EXISTS '%s' AS %s"
                           escaped-path
                           escaped-alias))
      database-code
      (catch clojure.lang.ExceptionInfo e
        (if (re-find #"(?i)lock" (.getMessage e))
          (throw (errors/resource-error
                  (str "Cache file is locked by another process: " file-path)
                  :cache-file))
          (throw e))))))

(defn attach-source-postgres!
  "Attach a PostgreSQL database as a source using postgres_scanner extension.
   Installs and loads the postgres extension if needed.
   Returns the source database alias (database-code__srcdb) on success.
   Throws IllegalArgumentException if database-code is invalid."
  [^TrexsqlDatabase db ^String database-code credentials]
  (ensure-open! db)
  (validate-identifier! database-code "database-code")
  (let [{:keys [host port database-name user password]} credentials
        alias (str database-code "__srcdb")
        escaped-alias (escape-identifier alias "source-alias")
        escaped-host (str/replace (str host) "'" "''")
        escaped-dbname (str/replace (str database-name) "'" "''")
        escaped-user (str/replace (str user) "'" "''")
        escaped-password (str/replace (str password) "'" "''")
        conn-string (format "host=%s port=%d dbname=%s user=%s password=%s"
                            escaped-host port escaped-dbname escaped-user escaped-password)]
    (load-extension! db "postgres")
    (execute! db (format "ATTACH IF NOT EXISTS '%s' AS %s (TYPE postgres, READ_ONLY)"
                         conn-string escaped-alias))
    alias))

(defn attach-source-bigquery!
  "Attach a BigQuery dataset as a source using bigquery extension.
   Installs and loads the bigquery extension if needed.
   Returns the source database alias (database-code__srcdb) on success.
   Throws IllegalArgumentException if database-code is invalid."
  [^TrexsqlDatabase db ^String database-code credentials]
  (ensure-open! db)
  (validate-identifier! database-code "database-code")
  (let [{:keys [host database-name]} credentials
        alias (str database-code "__srcdb")
        escaped-alias (escape-identifier alias "source-alias")
        escaped-project (str/replace (str host) "'" "''")
        escaped-dataset (str/replace (str database-name) "'" "''")
        conn-string (format "project=%s dataset=%s" escaped-project escaped-dataset)]
    (load-extension! db "bigquery" :source "community")
    (execute! db (format "ATTACH IF NOT EXISTS '%s' AS %s (TYPE bigquery, READ_ONLY)"
                         conn-string escaped-alias))
    alias))

(defn is-attached?
  "Check if a database with the given alias is currently attached.
   Returns true if attached, false otherwise.
   Throws RuntimeException on SQL errors (T3.1.4 - don't silently swallow)."
  [^TrexsqlDatabase db ^String database-alias]
  (ensure-open! db)
  (let [results (query db "SELECT database_name FROM duckdb_databases()")]
    (boolean (some #(= database-alias (.get ^HashMap % "database_name")) results))))

(defn detach-database!
  "Detach a previously attached database.
   Throws validation-error if database-alias is invalid.
   Throws resource-error if the database is not attached."
  [^TrexsqlDatabase db ^String database-alias]
  (ensure-open! db)
  (let [escaped-alias (escape-identifier database-alias "database-alias")]
    (when-not (is-attached? db database-alias)
      (throw (errors/resource-error (str "Database is not attached: " database-alias) :database)))
    (execute! db (format "DETACH %s" escaped-alias))))

(defn query-with-params
  "Execute a parameterized SQL query and return results as ArrayList<HashMap>.
   Params is a vector of values to bind to ? placeholders in the SQL.
   Throws sql-error on SQLException.
   Throws resource-error if database is closed."
  [^TrexsqlDatabase db ^String sql params]
  (ensure-open! db)
  (try
    (with-open [stmt (.prepareStatement (:connection db) sql)]
      (doseq [[idx param] (map-indexed vector params)]
        (.setObject stmt (inc idx) param))
      (with-open [rs (.executeQuery stmt)]
        (result-set->list rs)))
    (catch SQLException e
      (throw (errors/sql-error (str "SQL error: " (.getMessage e)) sql e)))))

(defn execute-with-params!
  "Execute a parameterized non-query SQL statement (INSERT, UPDATE, DELETE).
   Params is a vector of values to bind to ? placeholders in the SQL.
   Returns the number of affected rows.
   Throws sql-error on SQLException.
   Throws resource-error if database is closed."
  [^TrexsqlDatabase db ^String sql params]
  (ensure-open! db)
  (try
    (with-open [stmt (.prepareStatement (:connection db) sql)]
      (doseq [[idx param] (map-indexed vector params)]
        (.setObject stmt (inc idx) param))
      (.executeUpdate stmt))
    (catch SQLException e
      (throw (errors/sql-error (str "SQL error: " (.getMessage e)) sql e)))))

(defn get-raw-connection
  "Get the underlying JDBC connection. Use with caution - prefer using
   query, execute!, etc. for normal operations.
   Throws resource-error if database is closed."
  ^Connection [^TrexsqlDatabase db]
  (ensure-open! db)
  (:connection db))
