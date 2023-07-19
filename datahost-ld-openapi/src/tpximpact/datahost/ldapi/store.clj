(ns tpximpact.datahost.ldapi.store)

(defprotocol ChangeStore
  "Represents a repository for document changes"
  (insert-append [this file]
    "Inserts a ring file upload into this store. Returns a key value which
     can be used to retrieve the append data from this store using get-append.

     The file parameter should be a map with at least the following keys:
     :tempfile - An IOFactory instance containing the append data
     :filename - The name of the uploaded file on teh request")

  (get-append [this append-key]
    "Retrieves append data using the key returned by insert-append. If the
    key is found in this store, an input stream positioned at the start of the
    append data is returned."))

