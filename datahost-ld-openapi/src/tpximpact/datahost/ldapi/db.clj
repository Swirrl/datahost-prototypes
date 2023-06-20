(ns tpximpact.datahost.ldapi.db
  "Note: current persistence is a temporary solution.

  Current persistence layer has same inteterface as Clojure's atom. At
  the moment we make use of the fact that each upsert results in an
  update of the atom value if the value didn't change. In other words:
  the atom's' value may be equal but not `identical?`, because the
  metadata is always updated with `:op` entry.
  status (`[:enum :noop :update :create]`)

  See: https://github.com/jimpil/duratom"
  (:require
   [duratom.core :as da]
   [integrant.core :as ig]
   [malli.core :as m]
   [meta-merge.core :as mm]
   [tpximpact.datahost.ldapi.models.shared :as models-shared]
   [tpximpact.datahost.ldapi.models.series :as series]
   [tpximpact.datahost.ldapi.models.release :as release]
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

(def ^:private UpsertInernalParams
  [:map
   [:op.upsert/keys
    [:or s.series/UpsertKeys s.release/UpsertKeys]]])

(def ^:private upsert-internal-params-valid? (m/validator UpsertInernalParams))

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
  "Returns a map {:op ... :jdonld-doc ...}, where :op conforms to
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
