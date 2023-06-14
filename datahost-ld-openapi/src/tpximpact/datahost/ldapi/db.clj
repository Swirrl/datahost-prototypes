(ns tpximpact.datahost.ldapi.db
  (:require
   [duratom.core :as da]
   [integrant.core :as ig]
   [meta-merge.core :as mm]
   [tpximpact.datahost.ldapi.models.series :as series]
   [tpximpact.datahost.ldapi.models.shared :as models-shared])
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

(defn upsert-series!
  "Returns a map {:op ... :jdonld-doc ...}"
  [db {:keys [series-slug] :as api-params} incoming-jsonld-doc]
  (let [series-key (models-shared/dataset-series-key series-slug)
        ;; TODO: could handle this more nicely after
        ;; https://github.com/Swirrl/datahost-prototypes/issues/57
        ;; by comparing modified/issued times..
        series-exists? (get @db series-key)
        op (if series-exists? :update :create)
        ts (ZonedDateTime/now (ZoneId/of "UTC"))
        updated-db (swap! db 
                          series/upsert-series 
                          (assoc api-params :op/timestamp ts)
                          incoming-jsonld-doc)]
    {:op op
     :jsonld-doc (get updated-db series-key)}))
