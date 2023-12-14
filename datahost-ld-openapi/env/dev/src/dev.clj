(ns dev
  (:refer-clojure :exclude [test])
  (:require
   [clojure.repl :refer :all]
   [clojure.tools.namespace.repl :as tns]
   [integrant.repl :refer [go reset halt]]
   [tpximpact.datahost.sys :as sys]))

;; temp disable this line if working on the dev namespace, obviously.
(tns/disable-reload! (find-ns 'dev))

;; require scope capture as a side effect
(require 'sc.api)

(clojure.tools.namespace.repl/set-refresh-dirs "dev/src" "src" "test" "resources")

(def start go)

(integrant.repl/set-prep!
  #(sys/prep-config (sys/load-configs ["ldapi/base-system.edn" "ldapi/env.edn"])))
