(ns tpximpact.datahost.ldapi.store.sql.interface
  (:import (java.util.concurrent ExecutorService Future)))

(defprotocol SQLStoreCompatible
  (-make-select-observations-as [this prefix]
    "Returns a seq of tuple<actual-name,as-name>. 'as-name' is going
    to be used as an alias in a 'select' statement as in:

    ```sql
    select foo as \"My Column\" from observations;
    ```"))

(defn submit
  "Submit f to the executor service.

  Motivation: for some databases (eq. SQLite) we want to ensure
  there's only one writer. Returns a Future."
  (^Future [^ExecutorService executor ^Callable f]
   (.submit executor ^Callable f)))
