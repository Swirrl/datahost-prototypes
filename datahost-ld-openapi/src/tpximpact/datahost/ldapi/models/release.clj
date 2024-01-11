(ns tpximpact.datahost.ldapi.models.release
  (:require [malli.core :as m]
            [malli.error :as m.error]
            [integrant.core :as ig]
            [hugsql.core :as hugsql]
            [hugsql.adapter.next-jdbc :as next-adapter]
            [hugsql.parameters :refer [identifier-param-quote]]
            [next.jdbc :as jdbc]
            [tpximpact.datahost.ldapi.schemas.common :as s.common]
            [tpximpact.datahost.ldapi.store.sql.interface :as sql.interface]
            [tpximpact.datahost.ldapi.util.name-munging :as name-munging]))

(def ^:private adapter (next-adapter/hugsql-adapter-next-jdbc))
(def ^:private sql-file "tpximpact/datahost/ldapi/models/sql/release.sql")

(defn- setup-queries! []
  (hugsql/def-db-fns sql-file {:adapter adapter :quoting :ansi})
  (hugsql/def-sqlvec-fns sql-file {:adapter adapter :quoting :ansi}))

(setup-queries!)

(defn- release-uri->release-table [release-uri prefix] (->> release-uri name-munging/sanitize-name (str prefix)))

(defn- create-table*
  [db f {:keys [row-schema] :as params} options]
  (let [{:keys [original-column-names col-spec]} (name-munging/observation-column-defs-sql params)
        _ (assert (pos? (count col-spec)))
        col-spec (mapv (fn [tup] {:column (nth tup 1) :spec (nth tup 2)}) col-spec)
        col-spec (assoc-in col-spec [(dec (count col-spec)) :comma?] false)]
    (f db (assoc params :column-spec (map column-spec col-spec)) options)))

(defn create-observations-table
  ([db params] (create-observations-table db params {:quoting :ansi}))
  ([db {:keys [row-schema] :as params} options]
   (create-table* db -create-observations-table params options)))

(defn create-observations-import-table
  [db {:keys [row-schema] :as params} options]
  (create-table* db -create-observations-import-table params options))

(defn create-common-tables
  [db]
  (create-imports-table db))

(defn create-release-tables
  "Creates tables for the release specified by URI and row-schema."
  [db {:keys [row-schema release-uri] :as params} options]
  (assert (not (:release-id params)))
  (let [params (assoc params :release_id (name-munging/sanitize-name release-uri))]
    (create-observations-import-table db params options)
    (create-observations-table db params options)))

(def ImportObservationParams
  (m/schema [:map
             [:release_id :string]
             [:import_id :int]
             [:column-names [:sequential :any]]
             [:observations [:sequential :any]]]))

(defn import-observations
  "Adds observations to the 'import' table.
  
  See: [[Importobservationparams]] "
  [db params]
  (assert (m/validate ImportObservationParams params))
  (-import-observations db params))

(defn- sanitize-params
  [{:keys [release-uri] :as params} {:keys [store] :as options}]
  (cond-> params
    store (assoc :select-columns (sql.interface/-make-select-observations-as store "selected"))))

(def SelectObservationsArgs
  (m/schema [:tuple
             [:fn {:error/message "DB connection is required"} some?]
             [:map
              [:release-uri :datahost/uri]
              [:import_uid :any]]
             [:map
              ;;[:table-prefix {:optional true} :string]
              [:store {:optional false} [:fn #(satisfies? sql.interface/SQLStoreCompatible %)]]]]
            {:registry s.common/registry}))

(def ^:private select-observation-args-valid? (m/validator SelectObservationsArgs))

(defn- validate-select-observations-args
  [db+params+opts]
  (when-not (select-observation-args-valid? db+params+opts)
    (throw (ex-info "invalid arguments" {:explanation (->> db+params+opts
                                                           (m/explain SelectObservationsArgs)
                                                           (m.error/humanize))}))))

(defn select-imported-observations
  [db {:keys [release-uri] :as params} options]
  (validate-select-observations-args [db params options])
  (let [imports-table (release-uri->release-table release-uri "import::")
        params (assoc params :imports-table imports-table)]
   (-select-imported-observations db (sanitize-params params options))))

(defn select-observations
  [db {:keys [release-uri] :as params} options]
  (validate-select-observations-args [db params options])
  (let [observations-table (release-uri->release-table release-uri "observations::")
        params (assoc params :observations-table observations-table)]
   (-select-observations db (sanitize-params params options) options)))

(defn complete-import--copy-records
  [db {:keys [release-uri] :as params} {:keys [store] :as options}]
  (let [observations-table (release-uri->release-table release-uri "observations::")
        imports-table (release-uri->release-table release-uri "import::")
        params (assoc params
                      :observations-table observations-table
                      :imports-table imports-table
                      :select-columns (sql.interface/-make-select-observations-as store nil))]
    (-complete-import--copy-records db params options)))

(defn complete-import--delete-import-records
  [db {:keys [release-uri] :as params} options]
  (let [params (assoc params :imports-table (release-uri->release-table release-uri "import::"))]
   (-complete-import--delete-import-records db params options)))

(defmethod ig/init-key ::queries [_ _]
  (setup-queries!))

(defmethod ig/halt-key! ::queries [_ _])

(defn db-ok?
  [conn]
  (try
    (jdbc/execute-one! conn ["select count(*) from imports"])
    true
    (catch java.sql.SQLException ex
      false)))

(defmethod ig/init-key ::common-tables [_ {{:keys [db-config connection]} :db}]
  ;; if it's an in memory database, we attempt to use the already open connection,
  ;; otherwise it's a peristent DB, so we can open&close the connection.
  (let [{:keys [spec user password]} db-config]
    (if (and connection (not (db-ok? connection)))
      (create-common-tables connection)
      (with-open [conn (jdbc/get-connection spec user password)]
        (when-not (db-ok? conn)
          (create-common-tables conn))))))

(defmethod ig/halt-key! ::common-tables [_ _])
