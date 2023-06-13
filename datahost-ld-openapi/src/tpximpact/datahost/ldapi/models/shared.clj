(ns tpximpact.datahost.ldapi.models.shared
  (:require
   [clojure.set :as set]
   [malli.core :as m]
   [tpximpact.datahost.ldapi.util :as util])
  (:import
   [java.net URI]))

(def ld-root
  "For the prototype this item will come from config or be derived from
  it. It should have a trailing slash."
  (URI. "https://example.org/data/"))

(defn dataset-series-key [series-slug]
  (str (.getPath ld-root) series-slug))

(defn release-key [series-slug release-slug]
  (str (dataset-series-key series-slug) "/" release-slug))

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

(defn validate-context [ednld base]
  (if-let [base-in-doc (get-in ednld ["@context" 1 "@base"])]
    (if (= base base-in-doc)
      (update ednld "@context" normalise-context base)
      (throw (ex-info
              (str "@base must currently be set to '" base "'")
              {:type :validation-error
               :expected-value base
               :actual-value base-in-doc})))
    (update ednld "@context" normalise-context base)))

(def query-params->series-keys
  {:title "dcterms:title"
   :description "dcterms:description"})

(defn rename-query-params-to-series-keys
  [m]
  (set/rename-keys m query-params->series-keys))


(defn merge-params-with-doc [api-params jsonld-doc]
;; TODO NOW:: probably change this to prefer jsonld doc
  (let [merged-doc (merge jsonld-doc
                          (set/rename-keys api-params
                                           {:title "dcterms:title"
                                            :description "dcterms:description"}))]
    (util/dissoc-by-key merged-doc keyword?)))



;; TODO NOW double check these..
(def registry
  (merge
   (m/class-schemas)
   (m/comparator-schemas)
   (m/base-schemas)
   (m/type-schemas)
   {:slug-string [:and :string [:re {:error/message "should contain alpha numeric characters and hyphens only."}
                                       #"^[a-z,A-Z,\-,0-9]+$"]]
    :url-string (m/-simple-schema {:type :url-string :pred
                                   (fn [x]
                                     (and (string? x)
                                          (try (URI. x)
                                               true
                                               (catch Exception ex
                                                 false))))})}))

(defn validate-id [slug cleaned-doc]
  (let [id-in-doc (get cleaned-doc "@id")]
    (cond
      (nil? id-in-doc) cleaned-doc

      (= slug id-in-doc) cleaned-doc

      :else (throw
             (ex-info "@id should for now be expressed as a slugged style suffix, and if present match that supplied as the API slug."
                      {:supplied-id id-in-doc
                       :expected-id slug})))))
