(ns tpximpact.datahost.ldapi.store.file
  "Namespace for implementing a store for change data which uses the file system"
  (:require
    [clojure.tools.logging :as log]
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
  (let [buf (byte-array 2048)]
    (loop []
      (let [bytes-read (.read is buf)]
        (when-not (= -1 bytes-read)
          (recur))))))

(defn- maybe-reset-stream
  [input]
  (when (instance? java.io.InputStream input)
    (let [is ^java.io.InputStream input]
      (.reset is))))

(defn- file->digest
  "Computes a file digest as a string for input with the named digest
   algorithm.

  'input' can be whatever [[io/input-stream]] can handle."
  [input digest-alg]
  (let [digest (MessageDigest/getInstance digest-alg)]
    (with-open [is (DigestInputStream. (io/input-stream input) digest)]
      (consume-input-stream is)
      ;; we expect the caller `store/make-insert-request!` to supply a
      ;; stream that should be used repeatedly. At least 2 times:
      ;; 1. to calculate the digest 2. write it to file.
      ;; So we reset it here. If the InputStream implementation
      ;; throws on .reset(), it's probably a mistake to use it here
      (maybe-reset-stream input)
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
  (-insert-data-with-request [_ request]
    (let [{data :data digest :key} request
          _ (assert digest)
          location (file-location root-dir digest)]
      (when-not (.exists location)
        (.mkdirs (.getParentFile location))
        ;; copy input file into temp location within store then rename to
        ;; final destination
        (let [store-temp (File/createTempFile "filestore" nil root-dir)]
          (io/copy (:data request) store-temp)
          (.renameTo store-temp location)
          (log/debug  "-insert-data-with-request" {:type (type data) :key digest})))
      digest))
  
  (-insert-data [this {:keys [tempfile]}]
    (store/-insert-data-with-request this (store/make-insert-request! this tempfile)))

  (-get-data [_this data-key]
    (let [location (file-location root-dir (.toString data-key))]
      (if (.exists location)
        (io/input-stream location)      ;TODO: verify the strem is closed in all call sites
        (throw (ex-info "Data not found for key" {:key data-key})))))

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
