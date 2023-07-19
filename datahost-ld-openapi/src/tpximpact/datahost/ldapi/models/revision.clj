(ns tpximpact.datahost.ldapi.models.revision
  (:require
    [ring.util.io :as ring-io]
    [tablecloth.api :as tc]
    [tpximpact.datahost.ldapi.compact :as cmp]
    [tpximpact.datahost.ldapi.db :as db]
    [tpximpact.datahost.ldapi.resource :as resource]
    [tpximpact.datahost.ldapi.store :as store]))

(defn input-stream->dataset [is]
  (tc/dataset is {:file-type :csv}))

(defn csv-file-locations->dataset [change-store append-keys]
  (some->> append-keys
           (map #(input-stream->dataset (store/get-append change-store %)))
           (apply tc/concat)))

(defn write-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
   (fn [out-stream]
     (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn revision->csv-stream [triplestore change-store revision]
  (when-let [merged-datasets (csv-file-locations->dataset change-store
                                                          (db/revision-appends-file-locations triplestore revision))]
    (write-to-outputstream merged-datasets)))

(defn change->csv-stream [change-store change]
  (let [appends (resource/get-property1 change (cmp/expand :dh/appends))]
    (when-let [dataset (csv-file-locations->dataset change-store [appends])]
      (write-to-outputstream dataset))))

(defn release->csv-stream [triplestore change-store release]
  ;; TODO: loading of appends file locations could be done in one query
  (let [revision-uris (resource/get-property release (cmp/expand :dh/hasRevision))
        appends-file-keys (some->> revision-uris
                                   (map #(db/get-revision triplestore %))
                                   (map (partial db/revision-appends-file-locations triplestore))
                                   (flatten))]
    (when-let [merged-datasets (csv-file-locations->dataset change-store appends-file-keys)]
      (write-to-outputstream merged-datasets))))
