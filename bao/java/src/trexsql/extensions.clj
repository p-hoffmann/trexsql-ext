(ns trexsql.extensions
  "DuckDB extension discovery and loading."
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]
           [java.sql Connection Statement SQLException]))

(defn has-avx-support?
  "Check if the CPU supports AVX instructions.
   Reads /proc/cpuinfo on Linux."
  []
  (try
    (let [cpuinfo (slurp "/proc/cpuinfo")]
      (boolean (re-find #"\bavx\b" cpuinfo)))
    (catch Exception _
      false)))

(defn- extension-file?
  [^File f]
  (and (.isFile f)
       (or (str/ends-with? (.getName f) ".duckdb_extension")
           (str/ends-with? (.getName f) ".trex"))))

(defn- extension-name
  [^File f]
  (str/replace (.getName f) #"\.(duckdb_extension|trex)$" ""))

(defn find-extensions
  "Find all DuckDB extension files in the given directory.
   Searches recursively in @trex subdirectories.
   Returns seq of {:name <string> :path <string> :requires-avx <boolean>}"
  [extensions-path]
  (let [base-dir (io/file extensions-path)]
    (when (.isDirectory base-dir)
      (for [subdir (.listFiles base-dir)
            :when (.isDirectory subdir)
            ext-file (.listFiles subdir)
            :when (extension-file? ext-file)
            :let [name (extension-name ext-file)]]
        {:name name
         :path (.getAbsolutePath ext-file)
         :requires-avx (= name "llama")}))))

(defn load-extension
  "Load a single extension into the DuckDB connection.
   Returns {:name <string> :loaded <boolean> :error <string or nil>}"
  [^Connection conn {:keys [name path requires-avx]}]
  (let [avx-available? (has-avx-support?)]
    (cond
      ;; Skip llama if no AVX support
      (and requires-avx (not avx-available?))
      (do
        (println (str "Skipping " name " extension (no AVX support)"))
        {:name name :loaded false :error "No AVX support"})

      :else
      (try
        (println (str "Loading extension: " name))
        (with-open [stmt (.createStatement conn)]
          (.execute stmt (str "LOAD '" path "'")))
        {:name name :loaded true :error nil}
        (catch SQLException e
          (println (str "Failed to load extension: " path))
          (println (str "  Error: " (.getMessage e)))
          {:name name :loaded false :error (.getMessage e)})))))

(defn load-extensions
  "Load all extensions from the configured directory.
   Returns set of successfully loaded extension names."
  [^Connection conn extensions-path]
  (let [extensions (find-extensions extensions-path)]
    (if (empty? extensions)
      (do
        (println (str "Warning: Could not open extensions directory: " extensions-path))
        #{})
      (let [results (map #(load-extension conn %) extensions)
            loaded (filter :loaded results)]
        (set (map :name loaded))))))

(defn loaded-extensions
  "Return set of loaded extension names from database state."
  [db]
  (:extensions-loaded db))
