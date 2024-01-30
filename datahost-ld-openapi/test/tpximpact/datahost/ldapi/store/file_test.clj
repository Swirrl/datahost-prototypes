(ns tpximpact.datahost.ldapi.store.file-test
  (:require [clojure.test :as t]
            [tpximpact.datahost.ldapi.files :as files]
            [tpximpact.datahost.ldapi.store :as store]
            [tpximpact.datahost.ldapi.store.file :as fstore]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]])
  (:import [java.lang AutoCloseable]
           [java.io File]))

(defrecord TempFiles [files]
  AutoCloseable
  (close [_this]
    (doseq [f @files]
      (.delete f))))

(defrecord TempDir [dir]
  AutoCloseable
  (close [_this]
    (files/delete-dir dir)))

(defn temp-dir []
  (let [dir (files/create-temp-directory "datahost")]
    (->TempDir dir)))

(defn new-temp-file [{:keys [files]} prefix]
  (let [f (File/createTempFile prefix nil)]
    (swap! files conj f)
    f))

(defn temp-files []
  (->TempFiles (atom [])))

(defn- add-to-store [store files m contents]
  (let [f (new-temp-file files "filestore-test")]
    (spit f contents)
    (let [key (store/request-data-insert store (store/make-insert-request! store f))]
      (assoc m key contents))))

(def file-contents-gen gen/string)

(def prop-added-data-fetched-by-key
  (prop/for-all [file-contents (gen/vector file-contents-gen)]
    (with-open [temp-dir (temp-dir)]
      (let [store (fstore/->FileChangeStore (:dir temp-dir))]
        (with-open [fs (temp-files)]
          (let [added (reduce (fn [acc contents]
                                (add-to-store store fs acc contents))
                              {}
                              file-contents)
                fetched (into {} (map (fn [k] [k (slurp (store/get-data store k))]) (keys added)))]
            (= added fetched)))))))

(defspec added-data-can-be-fetched-by-key
  100
  prop-added-data-fetched-by-key)
