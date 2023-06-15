(ns tpximpact.datahost.ldapi.models.shared
  (:require
   [clojure.set :as set]
   [malli.core :as m]
   [tpximpact.datahost.ldapi.util :as util]
   [tpximpact.datahost.ldapi.errors :as api-errors])
  (:import
   [java.net URI]
   [java.time ZonedDateTime]))

(def ld-root
  "For the prototype this item will come from config or be derived from
  it. It should have a trailing slash."
  (URI. "https://example.org/data/"))

;;; ---- KEY CTORS

(defn dataset-series-key [series-slug]
  (str (.getPath ld-root) series-slug))

(defn release-key [series-slug release-slug]
  (str (dataset-series-key series-slug) "/" release-slug))

;;; ---- CONTEXT OPS

(defn normalise-context [ednld base]
  (let [normalised-context ["https://publishmydata.com/def/datahost/context"
                            {"@base" base}]]
    (if-let [context (get ednld "@context")]
      (cond
        (= context "https://publishmydata.com/def/datahost/context") normalised-context
        (= context normalised-context) normalised-context

        :else (throw (ex-info "Invalid @context" {:supplied-context context
                                                  :valid-context normalised-context})))

      ;; return normalised-context if none provided
      normalised-context)))

(defn validate-context 
  "Returns modified LD document or throws."
  [ednld base]
  (if-let [base-in-doc (get-in ednld ["@context" 1 "@base"])]
    (if (= base base-in-doc)
      (update ednld "@context" normalise-context base)
      (throw (ex-info
              (str "@base must currently be set to '" base "'")
              {:type :validation-error
               :expected-value base
               :actual-value base-in-doc})))
    (update ednld "@context" normalise-context base)))

;;; ---- DOCUMENT OPS

(def query-params->common-keys
  {:title "dcterms:title"
   :description "dcterms:description"})

(defn rename-query-params-to-common-keys
  [m]
  (set/rename-keys m query-params->common-keys))

(defn merge-params-with-doc 
  "Merges the api params with document (api params last).
  
  Renames the api params keys to appropriate document keys."
  [api-params jsonld-doc]
  (-> jsonld-doc
      (merge (rename-query-params-to-common-keys api-params))
      (util/dissoc-by-key keyword?)))

(defn validate-id 
  "Returns unchanged doc or throws.

  Thrown exception's `ex-data` will contain :supplied-id, :expected-id"
  [slug cleaned-doc]
  (let [id-in-doc (get cleaned-doc "@id")]
    (cond
      (nil? id-in-doc) cleaned-doc

      (= slug id-in-doc) cleaned-doc

      :else (throw
             (ex-info "@id should for now be expressed as a slugged style suffix, and if present match that supplied as the API slug."
                      {:type ::api-errors/validation-failure
                       :supplied-id id-in-doc
                       :expected-id slug})))))


;;; ---- ISSUED+MODIFIED DATES

(def ^:private date-formatter
  java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME)

(defmulti -issued+modified-dates
  "Adjusts the 'dcterms:issued' and 'dcterms:modified' of the document.
  Ensures that the new document does not modify the issue date.

  Behaviour differs when issuing a new seris and when modifying an
  existing one."
  (fn [_api-params old-doc _new-doc]
    (case (some? old-doc)
      false :issue
      true  :modify)))

(defmethod -issued+modified-dates :issue
  [{^ZonedDateTime timestamp :op/timestamp} _ new-doc]
  (let [ts-string (.format timestamp date-formatter)]
    (assoc new-doc
           "dcterms:issued" ts-string
           "dcterms:modified" ts-string)))

(defmethod -issued+modified-dates :modify
  [{^ZonedDateTime timestamp :op/timestamp} old-doc new-doc]
  (assoc new-doc
         "dcterms:issued" (get old-doc "dcterms:issued")
         "dcterms:modified" (.format timestamp date-formatter)))

(defn issued+modified-dates
  [{timestamp :op/timestamp :as api-params} old-doc new-doc]
  {:pre [(instance? java.time.ZonedDateTime timestamp)]}
  (-issued+modified-dates api-params old-doc new-doc))

