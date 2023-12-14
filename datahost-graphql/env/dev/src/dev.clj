(ns dev
  (:refer-clojure :exclude [test])
  (:require
    [clojure.repl :refer :all]
    [clojure.java.io :as io]
    [clojure.tools.namespace.repl :refer [refresh] :as tns]
    [grafter-2.rdf4j.repository :as repo]
    [integrant.repl :refer [clear halt go init prep reset]]
    [integrant.core :as ig]
    [integrant.repl.state :refer [config system]]
    [tpximpact.sys :as sys]))

;; temp disable this line if working on the dev namespace, obviously.
(tns/disable-reload! (find-ns 'dev))

;; require scope capture as a side effect
(require 'sc.api)

(clojure.tools.namespace.repl/set-refresh-dirs "dev/src" "src" "test" "resources")

(def start go)

(def fixture-repo (repo/fixture-repo (io/resource "fixture-data.ttl")))

(defmethod ig/init-key :dev/fixture-repo [_ _]
  (constantly fixture-repo))

(integrant.repl/set-prep!
  #(sys/prep-config (sys/load-configs ["catql/base-system.edn"])))
