(ns tpximpact.datahost.ldapi.routes.release
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.copy :as copy]))

(defn get-release-route-config [db]
  {:summary copy/get-release-summary
   :handler (partial handlers/get-release db)})

(defn put-release-route-config [db]
  {:summary copy/put-release-summary
   :handler (partial handlers/put-release db)}
  )
