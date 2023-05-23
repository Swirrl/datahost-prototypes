(ns tpximpact.datahost.ldapi.db
  (:require
   [duratom.core :as da]
   [integrant.core :as ig]))

(defmethod ig/init-key :tpximpact.datahost.ldapi.db/db [_ {:keys [storage-type opts]}]
  (da/duratom storage-type opts))
