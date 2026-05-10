(ns trexsql.agent.tools.search-phenotypes
  "search_phenotypes tool — queries PheKB, OHDSI Forums (Discourse), and the
   bundled OHDSI Phenotype Library CSV in parallel, merges results, returns
   up to 5 hits per source.

   Mirrors AtlasNeo's search-phenotypes tool; ports the three independent
   data sources verbatim."
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(def ^:private per-source-limit 5)
(def ^:private http-timeout-ms 12000)

(defn- match-all-terms?
  "Case-insensitive: every word in `query` appears somewhere in `text`."
  [query text]
  (let [t (str/lower-case (or text ""))
        terms (->> (str/split (str/lower-case (or query "")) #"\s+")
                   (remove str/blank?))]
    (and (seq terms)
         (every? #(str/includes? t %) terms))))

(defn- search-phenotype-library [query]
  (try
    (let [resource (io/resource "phenotype-library/Cohorts.csv")]
      (if-not resource
        []
        (with-open [rdr (io/reader resource)]
          (let [lines (vec (line-seq rdr))
                rows (rest lines) ;; skip header
                hits (->> rows
                          (map (fn [line]
                                 (let [cols (str/split line #",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
                                       [cohortId cohortName logicDescription hashTag]
                                       (mapv #(str/replace (or % "") #"^\"|\"$" "") cols)]
                                   {:cohortId cohortId
                                    :cohortName cohortName
                                    :logicDescription logicDescription
                                    :hashTag hashTag})))
                          (filter (fn [r] (or (match-all-terms? query (:cohortName r))
                                              (match-all-terms? query (:logicDescription r))
                                              (match-all-terms? query (:hashTag r)))))
                          (take per-source-limit))]
            (mapv (fn [r]
                    {:source "OHDSI Phenotype Library"
                     :title (:cohortName r)
                     :description (:logicDescription r)
                     :url (str "https://raw.githubusercontent.com/OHDSI/PhenotypeLibrary/main/inst/cohorts/"
                              (:cohortId r) ".json")})
                  hits)))))
    (catch Exception e
      (log/warn e "phenotype-library lookup failed")
      [])))

(defn- search-phekb [query]
  (try
    (let [resp (http/get "https://phekb.org/services/phenotypes/views/phenotype_table.json"
                         {:query-params {:display_id "services_1"}
                          :as :json
                          :throw-exceptions false
                          :socket-timeout http-timeout-ms
                          :connection-timeout http-timeout-ms})
          rows (or (:body resp) [])
          hits (->> rows
                    (filter (fn [row]
                              (let [text (str (get row :title) " " (get row :description))]
                                (match-all-terms? query text))))
                    (take per-source-limit))]
      (mapv (fn [row]
              {:source "PheKB"
               :title (or (:title row) "Untitled")
               :description (or (:description row) "")
               :url (or (:url row) "https://phekb.org")})
            hits))
    (catch Exception e
      (log/warn e "phekb lookup failed")
      [])))

(defn- search-discourse [query]
  (try
    (let [resp (http/get "https://forums.ohdsi.org/search.json"
                         {:query-params {:q query}
                          :as :json
                          :throw-exceptions false
                          :socket-timeout http-timeout-ms
                          :connection-timeout http-timeout-ms})
          topics (or (get-in resp [:body :topics]) [])
          posts  (or (get-in resp [:body :posts]) [])
          post-by-topic (into {} (for [p posts] [(:topic_id p) p]))
          hits (->> topics
                    (take per-source-limit)
                    (map (fn [topic]
                           (let [post (post-by-topic (:id topic))]
                             {:source "OHDSI Forums"
                              :title (:title topic)
                              :description (or (:blurb post) "")
                              :url (str "https://forums.ohdsi.org/t/"
                                        (:slug topic) "/" (:id topic))}))))]
      (vec hits))
    (catch Exception e
      (log/warn e "discourse lookup failed")
      [])))

(defn run
  "Tool entrypoint. Args: {:query \"clinical condition\"}."
  [args _req]
  (let [query (str (or (:query args) ""))]
    (if (str/blank? query)
      {:results [] :note "empty query"}
      (let [all (concat (search-phenotype-library query)
                        (search-phekb query)
                        (search-discourse query))]
        {:results (vec all)}))))
