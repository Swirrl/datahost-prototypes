(ns tpximpact.datahost.ldapi.models.revision
  (:require
    [ring.util.io :as ring-io]
    [tablecloth.api :as tc]
    [tpximpact.datahost.ldapi.db :as db])
  (:import (java.io ByteArrayInputStream)))

(defn string->stream
  ([s] (string->stream s "UTF-8"))
  ([s encoding]
   (-> s
       (.getBytes encoding)
       (ByteArrayInputStream.))))

(defn csv-str->dataset [str]
  (some-> str
          (string->stream)
          (tc/dataset {:file-type :csv})))



(defn csv-file-locations->dataset [triplestore appends-file-locations]
  (some->> (first appends-file-locations)
           (map #(csv-str->dataset (db/get-file-contents triplestore %)))
           (remove nil?)
           (apply tc/concat)))

(defn write-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
   (fn [out-stream]
     (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn revision->csv-stream [triplestore revision]
  (when-let [merged-datasets (csv-file-locations->dataset triplestore (db/revision-appends-file-locations triplestore revision))]
    (write-to-outputstream merged-datasets)))

(defn change->csv-stream [triplestore change]
  (when-let [dataset (csv-file-locations->dataset triplestore [(get change "dh:appends")])]
    (write-to-outputstream dataset)))

(defn release->csv-stream [triplestore release]
  (let [revisions (some->> (get release "dh:hasRevision")
                           (map #(get @triplestore %)))
        appends-file-keys (some->> revisions
                                   (map (partial db/revision-appends-file-locations triplestore))
                                   (flatten))]
    (when-let [merged-datasets (csv-file-locations->dataset triplestore appends-file-keys)]
      (write-to-outputstream merged-datasets))))
