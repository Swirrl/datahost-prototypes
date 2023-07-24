(ns tpximpact.datahost.ldapi.store.temp-file-store
  (:require [clojure.test :as t]
            [integrant.core :as ig]
            [tpximpact.datahost.ldapi.files :as files]
            [tpximpact.datahost.ldapi.store :as store]
            [tpximpact.datahost.ldapi.store.file :as fstore])
  (:import [java.lang AutoCloseable]))

(defrecord TempFileStore [file-store]
  store/ChangeStore
  (insert-append [_this file]
    (store/insert-append file-store file))
  (get-append [_this append-key]
    (store/get-append file-store append-key))

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
