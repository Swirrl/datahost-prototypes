(ns tpximpact.datahost.ldapi.db
  "Note: current persistence is a temporary solution.

  Current persistence layer has same interface as Clojure's atom. At
  the moment we make use of the fact that each upsert results in an
  update of the atom value if the value didn't change. In other words:
  the atom's' value may be equal but not `identical?`, because the
  metadata is always updated with `:op` entry.
  status (`[:enum :noop :update :create]`)

  See: https://github.com/jimpil/duratom"
  (:require
    [clojure.string :as str]
    [duratom.core :as da]
    [integrant.core :as ig]
    [malli.core :as m]
    [meta-merge.core :as mm]
    [tpximpact.datahost.ldapi.models.shared :as models-shared]
    [tpximpact.datahost.ldapi.models.series :as series]
    [tpximpact.datahost.ldapi.models.release :as release]
    [tpximpact.datahost.ldapi.models.revision :as revision]
    [tpximpact.datahost.ldapi.models.change :as change]
    [tpximpact.datahost.ldapi.schemas.release :as s.release]
    [tpximpact.datahost.ldapi.schemas.series :as s.series])
  (:import
    [java.time ZoneId ZonedDateTime]))

(def db-defaults
  {:storage-type :local-file
   :opts {:commit-mode :sync
          :init {}}})

(defmethod ig/prep-key ::db
  [_ options]
  (mm/meta-merge db-defaults options))

(defmethod ig/init-key ::db [_ {:keys [storage-type opts]}]
    (da/duratom storage-type opts))

(defn get-series [db series-slug]
  (let [key (models-shared/dataset-series-key series-slug)]
    (get @db key)))

(defn get-release [db series-slug release-slug]
  (let [key (models-shared/release-key series-slug release-slug)]
    (get @db key)))

(defn get-revision [db series-slug release-slug revision-id]
  (let [key (models-shared/revision-key series-slug release-slug revision-id)]
    (get @db key)))

(def ^:private UpsertInternalParams
  [:map
   [:op.upsert/keys
    [:or s.series/UpsertKeys s.release/UpsertKeys]]])

(def ^:private upsert-internal-params-valid? (m/validator UpsertInternalParams))

(defn- upsert-doc!
  "Applies upsert of the JSON-LD document and mutates the db-ref.
  Returns the value of the db-ref after the upsert."
  [db-ref update-fn api-params incoming-jsonld-doc]
  {:pre [(upsert-internal-params-valid? api-params)]}
  (let [ts (ZonedDateTime/now (ZoneId/of "UTC"))]
    (swap! db-ref
           update-fn
           (assoc api-params :op/timestamp ts)
           incoming-jsonld-doc)))

(defn upsert-series!
  "Returns a map {:op ... :jsonld-doc ...}, where :op conforms to
  `tpximpact.datahost.ldapi.schemas.api/UpsertOp`"
  [db {:keys [series-slug] :as api-params} incoming-jsonld-doc]
  (let [series-key (models-shared/dataset-series-key series-slug)
        api-params (assoc api-params :op.upsert/keys {:series series-key})
        updated-db (upsert-doc! db series/upsert-series api-params incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :jsonld-doc (get updated-db series-key)}))

(defn upsert-release!
  "Returns a map {:op ... :jsonld-doc ...} where :op conforms to
  `tpximpact.datahost.ldapi.schemas.api/UpsertOp`"
  [db {:keys [series-slug release-slug] :as api-params} incoming-jsonld-doc]
  (let [release-key (models-shared/release-key series-slug release-slug)
        api-params (assoc api-params :op.upsert/keys
                          {:series (models-shared/dataset-series-key series-slug)
                           :release release-key})
        updated-db (upsert-doc! db release/upsert-release api-params incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :jsonld-doc (get updated-db release-key)}))

(defn new-child-id [db parent-key child-predicate]
  "Looks at child keys on parent collection. Assumes keys are strings of format
  /x/y/.../1, i.e. paths that end in a stringified integer."
  (let [child-keys (get-in @db [parent-key child-predicate])]
    (if (empty? child-keys)
      1
      (-> child-keys
          sort
          last
          (str/split #"/")
          last
          (Integer/parseInt)
          inc))))

(defn insert-revision! [db {:keys [series-slug release-slug] :as api-params} incoming-jsonld-doc]
  (let [auto-revision-id (new-child-id db
                                       (models-shared/release-key series-slug release-slug)
                                       "dh:hasRevision")
        revision-key (models-shared/revision-key series-slug release-slug auto-revision-id)
        updated-db (swap! db revision/insert-revision api-params auto-revision-id incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :resource-id auto-revision-id
     :jsonld-doc (get updated-db revision-key)}))


(defn insert-change! [db {:keys [series-slug release-slug revision-id] :as api-params} incoming-jsonld-doc appends-tmp-file]
  (let [auto-change-id (new-child-id db
                                     (models-shared/revision-key series-slug release-slug revision-id)
                                     "dh:hasChange")
        change-key (models-shared/change-key series-slug release-slug revision-id auto-change-id)
        updated-db (swap! db change/insert-change api-params auto-change-id incoming-jsonld-doc appends-tmp-file)]
    {:op (-> updated-db meta :op)
     :resource-id auto-change-id
     :jsonld-doc (get updated-db change-key)}))


