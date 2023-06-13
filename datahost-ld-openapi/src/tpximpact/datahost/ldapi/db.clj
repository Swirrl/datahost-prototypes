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

(defn exists? [db key]
  (get @db key))

(defn get-op [db key]
  (if (exists? db key) :update :create))

(defn upsert-series!
  "Returns a map {:op ... :jdonld-doc ...}"
  [db {:keys [series-slug] :as api-params} incoming-jsonld-doc]
  (let [series-key (models-shared/dataset-series-key series-slug)
        ;; TODO: could handle this more nicely after
        ;; https://github.com/Swirrl/datahost-prototypes/issues/57
        ;; by comparing modified/issued times..
        op (get-op db series-key)
        ts (ZonedDateTime/now (ZoneId/of "UTC"))
        updated-db (swap! db
                          series/upsert-series
                          (assoc api-params :op/timestamp ts)
                          incoming-jsonld-doc)]
    {:op op
     :jsonld-doc (get updated-db series-key)}))

(defn upsert-release! [db {:keys [series-slug release-slug] :as api-params} incoming-jsonld-doc]
  (let [release-key (models-shared/release-key series-slug release-slug)
        op (get-op db release-key)
        updated-db (swap! db release/upsert-release api-params incoming-jsonld-doc)]
    {:op op
     :jsonld-doc (get updated-db release-key)}))
