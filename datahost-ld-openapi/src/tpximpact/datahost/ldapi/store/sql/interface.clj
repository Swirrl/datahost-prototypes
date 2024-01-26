(ns tpximpact.datahost.ldapi.store.sql.interface
  (:import (java.util.concurrent ExecutorService Future)))

(defprotocol SQLStoreCompatible
  (-make-select-observations-as [this prefix]
    "Returns a seq of tuple<actual-name,as-name>."))


(defn submit
  "Submit f to the executor service.

  Motivation: for some databases (eq. SQLite) we want to ensure
  there's only one writer. Returns a Future."
  (^Future [^ExecutorService executor ^Callable f]
   (.submit executor ^Callable f)))
