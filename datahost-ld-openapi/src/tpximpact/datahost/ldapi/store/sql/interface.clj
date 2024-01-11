(ns tpximpact.datahost.ldapi.store.sql.interface)

(defprotocol SQLStoreCompatible
  (-make-select-observations-as [this prefix]
    "Returns a seq of tuple<actual-name,as-name>."))
