(ns tpximpact.datahost.ldapi.routes.shared
  (:require
   [clojure.string :as string]
   [malli.core :as m]
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.schemas.common :as s.common]
   [ring.util.request :as request]
   [tpximpact.datahost.system-uris :as su]
   [tpximpact.datahost.ldapi.db :as db]))


;; Reitit map specs in reitit's vector format associating a parameter
;; key with a spec-fn and its metadata
(def series-slug-param-spec
  [:series-slug [:string {:description "A URI safe identifier which is unique within its URI namespace/prefix, used to identify the dataset-series."}]])

(def release-slug-param-spec
  [:release-slug [:string {:description "A URI safe identifier which is unique within its URI namespace/prefix, used to identify a release within a dataset-series."}]])

(def extension-param-spec
  [:extension {:optional true} [:string {:description "A file format extension e.g. csv, json"}]])

(def revision-id-param-spec
  [:revision-id [:int {:description "The revision identifier.  _Note: Consuming applications should not make any assumptions about the format of this identifier and should treat it as opaque._"}]])

(def change-id-param-spec
  [:change-id [:int {:description "The change or commit identifier.  _Note: Consuming applications should not make any assumptions about the format of this identifier and should treat it as opaque._"}]])

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
  (m/schema
   [:map
    ["dcterms:title" {:optional false} :title-string]
    ["dcterms:description" {:optional false} :description-string]
    ["rdfs:comment" {:optional true} :string]
    ["dcterms:publisher" {:optional true} :datahost/url-string]
    ["dcat:theme" {:optional true} :datahost/url-string]
    ["dcterms:license" {:optional true} :datahost/url-string]
    ["dh:contactName" {:optional true} :string]
    ["dh:contactEmail" {:optional true} :email-string]
    ["dh:contactPhone" {:optional true} :string]]
   {:registry s.common/registry}))

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

(def CreateChangeInputQueryParams
  (m/schema
   [:map
    ["title" {:description "Title for the commit message." :optional true} :title-string]
    ["description" {:description "A description/message for the commit." :optional false} :description-string]]
   {:registry s.common/registry}))

(def CreateChangeHeaders
  (m/schema
   [:map
    ["content-type"
     {:error/message "content-type header is required"
      :optional false
      :json-schema/example "text/csv"}
     [:enum "text/csv"]]]))

(def LdSchemaInputColumn
  [:map
   ["@type" [:enum "dh:DimensionColumn" "dh:AttributeColumn" "dh:MeasureColumn"]]
   ["csvw:datatype" [:or :string :keyword]]
   ["csvw:name" :string]
   ["csvw:titles" {:optional true} [:or
                                    :string
                                    [:sequential :string]]]])

(def LdSchemaInput
  "Schema for new schema documents"
  (let [err-msg "Schema should have exactly 1 'dh:MeasureColumn', 1+ 'dh:DimensionColumn, 0+ 'dh:AttributeColumn'' "]
    (mu/merge
     JsonLdBase
     [:and
      [:map {:closed false}
       ["dh:columns" [:repeat {:min 1} LdSchemaInputColumn]]]
      [:fn {:error/message err-msg}
       (fn release-schema-check [v]
         (let [freqs (frequencies (map #(get % "@type") (get v "dh:columns")))]
           (and (= 1  (get freqs "dh:MeasureColumn" 0))
                (<= 1 (get freqs "dh:DimensionColumn" 0))
                (<= 0 (get freqs "dh:AttributeColumn" 0)))))]])))

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
                   :query (m/explainer
                           (m/schema [:map
                                      ["title" :title-string]
                                      ["description" {:optional true} :description-string]]
                                     {:registry s.common/registry}))}})

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
(def metadata-strip-re-pattern #"-metadata$")

(defn strip-metadata-uris [path-params]
  (->> path-params
       (map (fn [[k v]] [k (string/replace v metadata-strip-re-pattern "")]))
       (into {})))

(defn csvm-request? [{:keys [request-method uri] :as request}]
  (and (= request-method :get) (re-find metadata-re-pattern uri)))

(defn csvm-request
  [{:keys [triplestore system-uris]
    {:keys [request-method uri path-params] :as request} :request :as _context}]
  (let [stripped-path-params (strip-metadata-uris path-params)
        uri (su/dataset-release-uri* system-uris stripped-path-params)]
    ;; TODO: this should pull the resource metadata, not just check the
    ;; resource exists
    (if (db/resource-exists? triplestore uri)
      (csv-metadata-json-handler request)
      {:status 404 :body "Not found"})))
