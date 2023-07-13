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

(defn revision-appends-file-locations
  "Given a Revision as a hash map, returns appends file locations"
  [db revision]
  (some->> (get revision "dh:hasChange")
           ;; TODO: needs to be triplestore
           (map #(get @db %))
           (map #(get % "dh:appends"))))

(defn csv-file-locations->dataset [triplestore appends-file-locations]
  (some->> appends-file-locations
           (map #(csv-str->dataset (db/get-file-contents triplestore %)))
           (remove nil?)
           (apply tc/concat)))

(defn write-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
   (fn [out-stream]
     (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn revision->csv-stream [triplestore revision]
  (when-let [merged-datasets (csv-file-locations->dataset triplestore (revision-appends-file-locations triplestore revision))]
    (write-to-outputstream merged-datasets)))

(defn change->csv-stream [triplestore change]
  (when-let [dataset (csv-file-locations->dataset triplestore [(get change "dh:appends")])]
    (write-to-outputstream dataset)))

(defn release->csv-stream [db release]
  (let [revisions (some->> (get release "dh:hasRevision")
                           (map #(get @db %)))
        appends-file-keys (some->> revisions
                                   (map (partial revision-appends-file-locations db))
                                   (flatten))]
    (when-let [merged-datasets (csv-file-locations->dataset db appends-file-keys)]
      (write-to-outputstream merged-datasets))))
