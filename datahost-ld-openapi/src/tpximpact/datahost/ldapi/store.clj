(ns tpximpact.datahost.ldapi.store)


(defprotocol ChangeStore
  "Represents a repository for document changes"
  (-insert-data [this data]
    "Inserts a ring file upload into this store. Returns a key value which
     can be used to retrieve the append data from this store using `-get-data`.

     The file parameter should be a map with at least the following keys:
     :tempfile - An IOFactory instance containing the append data
     :filename - The name of the uploaded file on the request")

  (-insert-data-with-request [this request]
    "Because call to `-data-key` may be relatively expensive and
    involve IO we want to cache it. We do it by creating a 'request'
    which will contain the key but won't expose it to the user.")

  (-get-data [this data-key]
    "Retrieves append data using the key returned by `insert-data` If the
    key is found in this store, an input stream positioned at the start of the
    append data is returned.")

  (-data-key [this data]
    "Returns a unique key for the given data. The key can be used to
    look up if the store already contains the data. The returned value
    should be treated as an opaque object."))

(defn insert-data
  [store data]
  (-insert-data store data))

(deftype InsertRequest [data key]
  clojure.lang.ILookup
  (valAt [this k]
    (case k
      :key key
      :data data
      nil)))

(defn request-data-insert
  [store ^InsertRequest request]
  (-insert-data-with-request store request))

(defn get-data
  [store data-key]
  (-get-data store data-key))

(defn data-key
  [store data]
  (-data-key store data))

(defn make-insert-request!
  "Creates an insert request. This can be a potentially be an expensive
  operation. Data can be a File, InputStream, etc."
  [store data]
  (->InsertRequest data (data-key store data)))
