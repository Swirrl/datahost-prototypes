(ns tpximpact.datahost.ldapi.models.resources
  (:require [clojure.data.csv :as csv]
            [tpximpact.datahost.system-uris :as su])
  (:import [java.io StringWriter]
           [java.net URI]))

(defn create-series [slug properties]
  {::type :dh/DatasetSeries
   :slug slug
   :releases []
   :properties properties})

(defn create-release [slug properties]
  {::type :dh/Release
   :slug slug
   :properties properties})

(defn create-schema [properties]
  {::type :dh/TableSchema
   :properties properties})

(defn create-revision [properties]
  {::type :dh/Revision
   :properties properties})

(defn create-change [change-kind rows properties]
  {::type :dh/Change
   :change-kind change-kind
   :properties properties
   :rows rows})

(defn set-parent [child parent]
  (assoc child :parent parent))

(defn get-parent [resource]
  (:parent resource))

(defn change-csv [{:keys [rows] :as _change}]
  (with-open [w (StringWriter.)]
    (csv/write-csv w rows)
    (.flush w)
    (str w)))

(defmulti resource-path (fn [resource] (::type resource)))

(defmethod resource-path :dh/DatasetSeries [{:keys [slug]}]
  (str "/data/" slug))

(defmethod resource-path :dh/Release [{:keys [slug] :as release}]
  (str (resource-path (get-parent release)) "/release/" slug))

(defmethod resource-path :dh/TableSchema [schema]
  (str (resource-path (get-parent schema)) "/schema"))

(defmethod resource-path :dh/Revision [revision]
  (:location revision))

(defmethod resource-path :dh/Change [change]
  (:location change))

(defmulti create-resource-path (fn [resource] (::type resource)))

(defmethod create-resource-path :default [resource]
  (resource-path resource))

(defmethod create-resource-path :dh/Revision [revision]
  (str (resource-path (get-parent revision)) "/revisions"))

(defmethod create-resource-path :dh/Change [{:keys [change-kind] :as change}]
  (let [change-path (case change-kind
                      :dh/ChangeKindAppend "appends"
                      :dh/ChangeKindRetract "retractions"
                      :dh/ChangeKindCorrect "corrections")]
    (str (resource-path (get-parent change)) "/" change-path)))

(defmulti resource-uri (fn [_system-uris resource] (::type resource)))

(defmethod resource-uri :dh/DatasetSeries [system-uris {:keys [slug] :as _series}]
  (su/dataset-series-uri system-uris slug))

(defmethod resource-uri :dh/Release [system-uris {:keys [slug] :as release}]
  (let [series-uri (resource-uri system-uris (get-parent release))]
    (su/dataset-release-uri system-uris series-uri slug)))

(defmethod resource-uri :dh/TableSchema [system-uris schema]
  (let [{release-slug :slug :as release} (get-parent schema)
        {series-slug :slug :as _series} (get-parent release)]
    (su/release-schema-uri system-uris {:series-slug series-slug :release-slug release-slug})))

(defn- location->resource-uri [system-uris location]
  (let [^URI base-uri (su/rdf-base-uri system-uris)
        rel-path (.replaceFirst location "/data/" "")]
    (.resolve base-uri rel-path)))

(defmethod resource-uri :dh/Revision [system-uris {:keys [location] :as _revision}]
  (location->resource-uri system-uris location))

(defmethod resource-uri :dh/Change [system-uris {:keys [location] :as _change}]
  (location->resource-uri system-uris location))

(defn resource->doc [resource]
  (:properties resource))

(defmulti on-created (fn [resource _create-response] (::type resource)))

(defmethod on-created :dh/Revision [revision create-response]
  (assoc revision :location (get-in create-response [:headers "Location"])))

(defmethod on-created :dh/Change [change create-response]
  (assoc change :location (get-in create-response [:headers "Location"])))

(defmethod on-created :default [resource _create-response]
  resource)

(defmulti resource-create-method (fn [resource] (::type resource)))
(defmethod resource-create-method :default [_resource] :put)
(defmethod resource-create-method :dh/TableSchema [_schema] :post)

(defmethod resource-create-method :dh/Revision [_revision] :post)

(defmethod resource-create-method :dh/Change [_change] :post)
