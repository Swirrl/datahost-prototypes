(ns tpximpact.datahost.ldapi.store.triplestore
  "Namespace implementing a store for changes data which uses a triplestore"
  (:require
    [grafter-2.rdf.protocols :as pr]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.compact :as compact]
    [tpximpact.datahost.ldapi.db :as db]
    [tpximpact.datahost.ldapi.models.shared :as models-shared]
    [tpximpact.datahost.ldapi.resource :as resource]
    [tpximpact.datahost.ldapi.store :as store])
  (:import [java.io ByteArrayInputStream]))

(defn string->stream
  ([s] (string->stream s "UTF-8"))
  ([s encoding]
   (-> s
       (.getBytes encoding)
       (ByteArrayInputStream.))))

(defrecord TriplestoreChangeStore [triplestore]
  store/ChangeStore
  (insert-append [_this {:keys [tempfile filename] :as _file}]
    (let [append-data (slurp tempfile)
          file-uri (models-shared/new-dataset-file-uri filename)
          append-statements [(pr/->Triple file-uri
                                          (compact/expand :dh/fileContents)
                                          append-data)]]
      (with-open [conn (repo/->connection triplestore)]
        (pr/add conn append-statements))
      file-uri))

  (get-append [_this append-key]
    (let [file-uri append-key
          bgps [[file-uri :dh/fileContents '?contents]]
          q {:prefixes  db/prefixes
             :construct bgps
             :where     bgps}
          append-contents (-> (db/get-resource-by-construct-query triplestore q)
                              (resource/get-property1 (compact/expand :dh/fileContents)))]
      (string->stream append-contents))))

