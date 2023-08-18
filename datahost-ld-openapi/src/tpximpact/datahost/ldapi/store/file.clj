(ns tpximpact.datahost.ldapi.store.file
  "Namespace for implementing a store for change data which uses the file system"
  (:require
    [clojure.java.io :as io]
    [integrant.core :as ig]
    [tpximpact.datahost.ldapi.store :as store])
  (:import [java.io InputStream]
           [java.security DigestInputStream MessageDigest]
           [java.util HexFormat]
           [java.io File]))

(defn- consume-input-stream
  "Reads an input stream to the end"
  [^InputStream is]
  (let [buf (byte-array 4096)]
    (loop []
      (let [bytes-read (.read is buf)]
        (when-not (= -1 bytes-read)
          (recur))))))

(defn- file->digest
  "Computes a file digest as a string for a file with the named digest
   algorithm"
  [file digest-alg]
  (let [digest (MessageDigest/getInstance digest-alg)]
    (with-open [is (DigestInputStream. (io/input-stream file) digest)]
      (consume-input-stream is)
      (let [^bytes bytes (.digest digest)]
        (.formatHex (HexFormat/of) bytes)))))

(defn- file-location
  "Returns the location of a file with the given digest within the file store"
  [root-dir ^String digest-str]
  (if (> (.length digest-str) 2)
    (let [dir (.substring digest-str 0 2)
          file (.substring digest-str 2)]
      (io/file root-dir dir file))
    (throw (ex-info "Invalid digest" {:digest digest-str}))))

(defrecord FileChangeStore [root-dir]
  store/ChangeStore
  (-insert-data-with-request [this request]
    (let [file-digest (:key request)
          location (file-location root-dir file-digest)]
      (when-not (.exists location)
        (.mkdirs (.getParentFile location))
        ;; copy input file into temp location within store then rename to
        ;; final destination
        (let [store-temp (File/createTempFile "filestore" nil root-dir)]
          (io/copy (:data request) store-temp)
          (.renameTo store-temp location)))
      file-digest))
  
  (-insert-data [this {:keys [tempfile]}]
    (store/-insert-data-with-request this (store/make-insert-request! this tempfile)))

  (-get-data [_this append-key]
    (let [location (file-location root-dir (.toString append-key))]
      (if (.exists location)
        (io/input-stream location)
        (throw (ex-info "Append not found for key" {:key append-key})))))

  (-data-key [_this data]
    (file->digest data "SHA-256")))

(defn get-root-dir
  "Returns the root directory of a file store"
  [fstore]
  (:root-dir fstore))

(defmethod ig/init-key ::store [_key {:keys [directory]}]
  (let [dir (io/file directory)]
    (.mkdirs dir)
    (->FileChangeStore dir)))
