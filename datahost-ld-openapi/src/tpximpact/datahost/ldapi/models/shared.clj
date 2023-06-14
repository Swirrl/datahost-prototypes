(ns tpximpact.datahost.ldapi.models.shared
  (:require
   [clojure.set :as set]
   [tpximpact.datahost.ldapi.util :as util])
  (:import
   [java.net URI]))

(def ld-root
  "For the prototype this item will come from config or be derived from
  it. It should have a trailing slash."
  (URI. "https://example.org/data/"))

(defn dataset-series-key [series-slug]
  (str (.getPath ld-root) series-slug))

(defn normalise-context [ednld]
  (let [normalised-context ["https://publishmydata.com/def/datahost/context"
                            {"@base" (str ld-root)}]]
    (if-let [context (get ednld "@context")]
      (cond
        (= context "https://publishmydata.com/def/datahost/context") normalised-context
        (= context normalised-context) normalised-context

        :else (throw (ex-info "Invalid @context" {:supplied-context context
                                                  :valid-context normalised-context})))

      ;; return normalised-context if none provided
      normalised-context)))

(def query-params->series-keys
  {:title "dcterms:title"
   :description "dcterms:description"})

(defn rename-query-params-to-series-keys
  [m]
  (set/rename-keys m query-params->series-keys))

(defn merge-params-with-doc [api-params jsonld-doc]
  (let [merged-doc (merge jsonld-doc
                          (set/rename-keys api-params
                                           {:title "dcterms:title"
                                            :description "dcterms:description"}))]
    (util/dissoc-by-key merged-doc keyword?)))
