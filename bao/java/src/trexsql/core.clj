(ns trexsql.core
  "Main entry point for Trexsql - Clojure DuckDB library."
  (:require [trexsql.db :as db]
            [trexsql.config :as config]
            [trexsql.extensions :as ext]
            [trexsql.servers :as servers])
  (:gen-class))

(def ^:private shutdown-promise (promise))
(def ^:private current-database (atom nil))

(defn add-shutdown-hook!
  "Register a JVM shutdown hook for graceful cleanup."
  [cleanup-fn]
  (.addShutdownHook
   (Runtime/getRuntime)
   (Thread. ^Runnable cleanup-fn)))

(defn shutdown!
  "Gracefully shutdown the database and any running servers."
  [database]
  (println "\n\nShutting down...")
  (when database
    (db/close! database))
  (reset! current-database nil))

(defn init
  "Initialize DuckDB database with extensions loaded.
   Config map can include :extensions-path to override default.
   Returns TrexsqlDatabase record."
  ([]
   (init {}))
  ([config]
   (let [merged-config (merge config/default-config config)
         extensions-path (config/get-extensions-path merged-config)
         conn (db/create-connection)
         loaded (ext/load-extensions conn extensions-path)
         database (-> (db/make-database conn merged-config)
                      (assoc :extensions-loaded loaded))]
     (reset! current-database database)
     database)))

(defn init-with-servers
  "Initialize DuckDB database and start Trexas/PgWire servers.
   Requires TREX_SQL_PASSWORD environment variable.
   Config map can override server ports, paths, etc.
   Returns TrexsqlDatabase record with servers running."
  ([]
   (init-with-servers {}))
  ([config]
   (let [database (init config)
         merged-config (merge config/default-config config)
         database-with-servers (servers/start-servers! database merged-config)]
     (reset! current-database database-with-servers)
     (servers/print-server-status merged-config)
     database-with-servers)))

(defn is-running?
  "Check if servers are currently running."
  [database]
  (boolean (:servers-running? database)))

(defn query
  "Execute a SQL query and return results.
   Wrapper around db/query for convenience."
  [database sql]
  (db/query database sql))

(defn execute!
  "Execute a non-query SQL statement.
   Wrapper around db/execute! for convenience."
  [database sql]
  (db/execute! database sql))

(defn loaded-extensions
  "Return set of loaded extension names."
  [database]
  (ext/loaded-extensions database))

(def main-help-text
  "Usage: trexsql <command> [options]

Commands:
  serve      Start Trexas and PgWire servers (default)
  cache      Manage DuckDB caches from source databases
  bundle     Create an eszip bundle from TypeScript/JavaScript

Use 'trexsql <command> --help' for more information about a command.

Examples:
  trexsql serve --trexas-port 9876
  trexsql cache create -s source -j \"jdbc:...\" -S schema
  trexsql bundle -e main.ts -o output.eszip")

(defn- print-main-help []
  (println main-help-text))

(defn- run-serve [args]
  (let [{:keys [options errors]} (config/parse-args args)]
    (when (:help options)
      (println config/help-text)
      (System/exit 0))
    (when (seq errors)
      (binding [*out* *err*]
        (doseq [err errors]
          (println (str "Error: " err))))
      (System/exit 1))
    (println "\uD83E\uDD95 Starting TREX")
    (let [database (init-with-servers options)]
      (add-shutdown-hook!
       #(shutdown! @current-database))
      @shutdown-promise)))

(defn -main
  "Main entry point - routes to subcommands."
  [& args]
  (let [command (first args)
        sub-args (rest args)]
    (case command
      "serve" (run-serve sub-args)
      "cache" (do
                (require 'trexsql.cli)
                (let [run-cache (resolve 'trexsql.cli/run-cache)
                      {:keys [exit-code]} (run-cache sub-args)]
                  (System/exit (or exit-code 0))))
      "bundle" (do
                 (require 'trexsql.cli)
                 (let [run-bundle (resolve 'trexsql.cli/run-bundle)
                       {:keys [exit-code]} (run-bundle sub-args)]
                   (System/exit (or exit-code 0))))
      "--help" (do (print-main-help) (System/exit 0))
      "-h" (do (print-main-help) (System/exit 0))
      nil (run-serve [])
      (if (or (clojure.string/starts-with? (str command) "-")
              (clojure.string/starts-with? (str command) "--"))
        (run-serve args)
        (do
          (println (format "Unknown command: %s" command))
          (println "\nUse 'trexsql --help' for usage information.")
          (System/exit 1))))))
