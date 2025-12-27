(ns trexsql.jdbc-types
  "JDBC to DuckDB type mapping and value conversion."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import [java.sql ResultSet ResultSetMetaData Types]
           [java.time LocalDate LocalDateTime LocalTime]
           [java.math BigDecimal]
           [org.trex TrexSQLAppender]))

(def jdbc-to-duckdb-types
  "JDBC type codes to DuckDB type names. Loaded from jdbc-type-mappings.edn."
  (if-let [resource (io/resource "jdbc-type-mappings.edn")]
    (edn/read-string (slurp resource))
    {Types/BIGINT     "BIGINT"
     Types/INTEGER    "INTEGER"
     Types/SMALLINT   "SMALLINT"
     Types/TINYINT    "TINYINT"
     Types/DECIMAL    "DECIMAL"
     Types/NUMERIC    "DECIMAL"
     Types/DOUBLE     "DOUBLE"
     Types/FLOAT      "FLOAT"
     Types/REAL       "REAL"
     Types/VARCHAR    "VARCHAR"
     Types/CHAR       "VARCHAR"
     Types/NVARCHAR   "VARCHAR"
     Types/NCHAR      "VARCHAR"
     Types/LONGVARCHAR "VARCHAR"
     Types/CLOB       "VARCHAR"
     Types/NCLOB      "VARCHAR"
     Types/DATE       "DATE"
     Types/TIME       "TIME"
     Types/TIMESTAMP  "TIMESTAMP"
     Types/BOOLEAN    "BOOLEAN"
     Types/BIT        "BOOLEAN"
     Types/BLOB       "BLOB"
     Types/BINARY     "BLOB"
     Types/VARBINARY  "BLOB"
     Types/NULL       "VARCHAR"}))

(defn get-duckdb-type
  "Map JDBC type code to DuckDB type. Defaults to VARCHAR."
  [jdbc-type]
  (let [duckdb-type (get jdbc-to-duckdb-types jdbc-type "VARCHAR")]
    (when-not (contains? jdbc-to-duckdb-types jdbc-type)
      (log/warn (format "Unmapped JDBC type: %d, defaulting to VARCHAR" jdbc-type)))
    duckdb-type))

(defrecord ColumnInfo [name jdbc-type duckdb-type precision scale nullable?])

(defn get-column-info
  "Extract column metadata from JDBC ResultSetMetaData."
  [^ResultSetMetaData metadata]
  (let [col-count (.getColumnCount metadata)]
    (vec
     (for [i (range 1 (inc col-count))]
       (let [jdbc-type (.getColumnType metadata i)]
         (->ColumnInfo
          (.getColumnName metadata i)
          jdbc-type
          (get-duckdb-type jdbc-type)
          (.getPrecision metadata i)
          (.getScale metadata i)
          (= (.isNullable metadata i) ResultSetMetaData/columnNullable)))))))

(defn- to-local-date [value]
  (cond
    (instance? LocalDate value) value
    (instance? java.sql.Date value) (.toLocalDate ^java.sql.Date value)
    (instance? java.util.Date value) (-> ^java.util.Date value .toInstant (.atZone (java.time.ZoneId/systemDefault)) .toLocalDate)
    (string? value) (LocalDate/parse value)
    :else (LocalDate/parse (str value))))

(defn- to-local-time [value]
  (cond
    (instance? LocalTime value) value
    (instance? java.sql.Time value) (.toLocalTime ^java.sql.Time value)
    (string? value) (LocalTime/parse value)
    :else (LocalTime/parse (str value))))

(defn- to-local-datetime [value]
  (cond
    (instance? LocalDateTime value) value
    (instance? java.sql.Timestamp value) (.toLocalDateTime ^java.sql.Timestamp value)
    (instance? java.util.Date value) (-> ^java.util.Date value .toInstant (.atZone (java.time.ZoneId/systemDefault)) .toLocalDateTime)
    (string? value) (LocalDateTime/parse value)
    :else (LocalDateTime/parse (str value))))

(defn append-typed-value!
  "Append value to TrexSQL Appender with type conversion."
  [^TrexSQLAppender appender value duckdb-type]
  (if (nil? value)
    (.appendNull appender)
    (case duckdb-type
      "BIGINT"    (.append appender (long value))
      "INTEGER"   (.append appender (int value))
      "SMALLINT"  (.append appender (short value))
      "TINYINT"   (.append appender (byte value))
      "DOUBLE"    (.append appender (double value))
      "FLOAT"     (.append appender (float value))
      "REAL"      (.append appender (float value))
      "DECIMAL"   (.append appender (if (instance? BigDecimal value)
                                       value
                                       (BigDecimal. (str value))))
      "VARCHAR"   (.append appender (str value))
      "BOOLEAN"   (.append appender (boolean value))
      "DATE"      (.append appender (to-local-date value))
      "TIME"      (.append appender (to-local-time value))
      "TIMESTAMP" (.append appender (to-local-datetime value))
      "BLOB"      (.append appender (if (bytes? value)
                                       value
                                       (.getBytes (str value) "UTF-8")))
      (.append appender (str value)))))

(defn read-typed-value
  "Read value from ResultSet with type handling. Returns nil for SQL NULL."
  [^ResultSet rs ^long col-idx duckdb-type]
  (let [value (case duckdb-type
                "BIGINT"    (.getLong rs col-idx)
                "INTEGER"   (.getInt rs col-idx)
                "SMALLINT"  (.getShort rs col-idx)
                "TINYINT"   (.getByte rs col-idx)
                "DOUBLE"    (.getDouble rs col-idx)
                "FLOAT"     (.getFloat rs col-idx)
                "REAL"      (.getFloat rs col-idx)
                "DECIMAL"   (.getBigDecimal rs col-idx)
                "VARCHAR"   (.getString rs col-idx)
                "BOOLEAN"   (.getBoolean rs col-idx)
                "DATE"      (.getDate rs col-idx)
                "TIME"      (.getTime rs col-idx)
                "TIMESTAMP" (.getTimestamp rs col-idx)
                "BLOB"      (.getBytes rs col-idx)
                (.getObject rs col-idx))]
    (when-not (.wasNull rs)
      value)))
