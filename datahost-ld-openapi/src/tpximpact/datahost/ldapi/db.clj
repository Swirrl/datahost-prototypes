(ns tpximpact.datahost.ldapi.db
  (:require [duratom.core :as da]))

(def db (da/duratom :local-file
                    :file-path ".datahostdb.edn"
                    :commit-mode :sync
                    :init {}))
