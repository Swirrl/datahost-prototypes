(ns tpximpact.datahost.ldapi.db
  (:require
   [duratom.core :as da]
   [integrant.core :as ig]
   [meta-merge.core :as mm]
   [tpximpact.datahost.ldapi.models.shared :as models-shared]
   [tpximpact.datahost.ldapi.models.series :as series]
   [tpximpact.datahost.ldapi.models.release :as release])
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

(defn- upsert-doc!
  "Applies upsert of the JSON-LD document and mutates the db-ref.
  Returns the value of the db-ref after the upsert."
  [db-ref update-fn api-params incoming-jsonld-doc]
  (let [ts (ZonedDateTime/now (ZoneId/of "UTC"))]
    (swap! db-ref
           update-fn
           (assoc api-params :op/timestamp ts)
           incoming-jsonld-doc)))

(defn upsert-series!
  "Returns a map {:op ... :jdonld-doc ...}"
  [db {:keys [series-slug] :as api-params} incoming-jsonld-doc]
  (let [series-key (models-shared/dataset-series-key series-slug)
        updated-db (upsert-doc! db series/upsert-series api-params incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :jsonld-doc (get updated-db series-key)}))

(defn upsert-release! 
  "Returns a map {:op ... :jsonld-doc ...}"
  [db {:keys [series-slug release-slug] :as api-params} incoming-jsonld-doc]
  (let [release-key (models-shared/release-key series-slug release-slug)
        updated-db (upsert-doc! db release/upsert-release api-params incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :jsonld-doc (get updated-db release-key)}))
