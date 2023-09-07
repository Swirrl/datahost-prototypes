(ns tpximpact.datahost.ldapi.routes.shared
  (:require
   [clojure.string :as string]
   [malli.core :as m]
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.schemas.common :as s.common]
   [ring.util.request :as request]
   [tpximpact.datahost.system-uris :as su]
   [tpximpact.datahost.ldapi.db :as db]))

(def NotFoundErrorBody
  "Default body for 404 errors, for 'application/[ld+]json and others."
  [:or
   [:re "Not found"]
   [:map ["message" [:re "Not found"]]]])

(def JsonLdBase
  "Common entries in JSON-LD documents.

  Note: we don't require users to pass @context, since we accept a
  relaxed JSON-LD subset."
  [:map {:closed false}
   ["dcterms:title" {:optional true} string?]
   ["dcterms:description" {:optional true} string?]
   ["@type" {:optional true} string?]
   ["@id" {:optional true} string?]
   ["@context" {:optional true}
    [:or :string [:tuple :string [:map {:closed false}
                                  ["@base" string?]]]]]])

(def ^:private
  required-input-fragment
  (m/schema
   [:map
    ["dcterms:title" {:optional false} :title-string]
    ["dcterms:description" {:optional false} :description-string]]
   {:registry s.common/registry}))

(def CreateSeriesInput
  "Input schema for creating a new series."
  required-input-fragment)

(def CreateReleaseInput
  "Input schema for creating a new release."
  required-input-fragment)

(def CreateRevisionInput
  "Input schema for creating new revision."
  (m/schema
   [:map
    ["dcterms:title" {:optional false} :title-string]
    ["dcterms:description" {:optional true} :description-string]]
   {:registry s.common/registry}))

(def CreateChangeInput
  "Input schema for new Changes."
  (m/schema
   [:map
    ["dcterms:title" {:optional true} :title-string]
    ["dcterms:description" {:optional false} :description-string]
    ["dcterms:format" {:optional false :json-schema/example "text/csv"} :string]]
   {:registry s.common/registry}))

(def LdSchemaInputColumn
  [:map
   ["csvw:datatype" [:or :string :keyword]]
   ["csvw:name" :string]
   ["csvw:titles" [:or
                   :string
                   [:sequential :string]]]
   ["@type" {:optional true} string?]])

(def LdSchemaInput
  "Schema for new schema documents"
  (mu/merge
   JsonLdBase
   [:map {:closed false}
    ["dh:columns" [:repeat {:min 1} LdSchemaInputColumn]]]))

;; TODO: create better resource representation
(def ResourceSchema
  [:string])

(def explainers
  {:put-series
   {:body (m/explainer [:maybe CreateSeriesInput])
    :query (m/explainer
            (m/schema [:map
                       ["title" :title-string]
                       ["description" :description-string]]
                      {:registry s.common/registry}))}

   :put-release {:body (m/explainer [:maybe CreateReleaseInput])
                 :query (m/explainer
                         (m/schema [:map
                                    ["title" :title-string]
                                    ["description" :description-string]]
                                   {:registry s.common/registry}))}
   :post-revision {:body (m/explainer [:maybe CreateRevisionInput])
                   :query (m/schema [:map
                                     ["title" :title-string]
                                     ["description" {:optional true} :description-string]]
                                    {:registry s.common/registry})}
   :post-revision-change {:body (m/explainer [:maybe CreateChangeInput])
                          :query (m/schema [:map
                                            ["title" {:optional true} :title-string]
                                            ["description" :description-string]]
                                           {:registry s.common/registry})}})

(defn base-url [request]
  (request/request-url (select-keys request [:scheme :headers :uri])))

(defn set-csvm-header [response request]
  (let [csvm-url (-> request (update :uri str "-metadata.json") base-url)]
    (update response :headers assoc "link" (str "<" csvm-url ">; "
                                                "rel=\"describedBy\"; "
                                                "type=\"application/csvm+json\""))))

(defn csv-metadata-json-handler [request]
  {:status 200
   :headers {"content-type" "application/csvm+json"}
   :body "{\"@context\": [\"http://www.w3.org/ns/csvw\", {\"@language\": \"en\"}]}"})

(def metadata-re-pattern #"-metadata.json$")

(defn strip-metadata-uris [path-params]
  (->> path-params
       (map (fn [[k v]] [k (string/replace v metadata-re-pattern "")]))
       (into {})))

(defn csvm-request? [{:keys [request-method uri] :as request}]
  (and (= request-method :get) (re-find metadata-re-pattern uri)))

(defn csvm-request
  [{:keys [triplestore system-uris]
    {:keys [request-method uri path-params] :as request} :request :as _context}]
  (let [path-params (strip-metadata-uris path-params)
        uri (su/dataset-release-uri* system-uris path-params)]
    ;; TODO: this should pull the resource metadata, not just check the
    ;; resource exists
    (if (db/resource-exists? triplestore uri)
      (csv-metadata-json-handler request)
      {:status 404 :body "Not found"})))
