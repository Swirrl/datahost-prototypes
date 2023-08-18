(ns tpximpact.datahost.ldapi.store.temp-file-store
  (:require [clojure.test :as t]
            [integrant.core :as ig]
            [tpximpact.datahost.ldapi.files :as files]
            [tpximpact.datahost.ldapi.store :as store]
            [tpximpact.datahost.ldapi.store.file :as fstore])
  (:import [java.lang AutoCloseable]))

(defrecord TempFileStore [file-store]
  store/ChangeStore
  (-insert-data [_this file]
    (store/insert-data file-store file))
  (-insert-data-with-request [_this request]
    (store/-insert-data-with-request file-store request))
  (-get-data [_this data-key]
    (store/get-data file-store data-key))
  (-data-key [_this data]
    (store/data-key file-store data))

  AutoCloseable
  (close [_]
    (let [dir (fstore/get-root-dir file-store)]
      (files/delete-dir dir))))

(defn create-temp-file-store []
  (let [dir (files/create-temp-directory "tempfilestore")
        fstore (fstore/->FileChangeStore dir)]
    (->TempFileStore fstore)))

(defmethod ig/init-key ::store [_key _opts]
  (create-temp-file-store))

(defmethod ig/halt-key! ::store [_key temp-store]
  (.close temp-store))
