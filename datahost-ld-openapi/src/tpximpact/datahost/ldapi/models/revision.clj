(ns tpximpact.datahost.ldapi.models.revision
  (:require
   [ring.util.io :as ring-io]
   [tablecloth.api :as tc])
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
           (map #(get @db %))
           (map #(get % "dh:appends"))))

(defn csv-file-locations->dataset [db appends-file-locations]
  (some->> appends-file-locations
           (map #(csv-str->dataset (get @db %)))
           (remove nil?)
           (apply tc/concat)))

(defn write-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
   (fn [out-stream]
     (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn revision->csv-stream [db revision]
  (when-let [merged-datasets (csv-file-locations->dataset db (revision-appends-file-locations db revision))]
    (write-to-outputstream merged-datasets)))

(defn change->csv-stream [db change]
  (when-let [dataset (csv-file-locations->dataset db [(get change "dh:appends")])]
    (write-to-outputstream dataset)))

(defn release->csv-stream [db release]
  (let [revisions (some->> (get release "dh:hasRevision")
                           (map #(get @db %)))
        appends-file-keys (some->> revisions
                                   (map (partial revision-appends-file-locations db))
                                   (flatten))]
    (when-let [merged-datasets (csv-file-locations->dataset db appends-file-keys)]
      (write-to-outputstream merged-datasets))))
