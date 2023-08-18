(ns tpximpact.datahost.ldapi.models.shared
  (:import
    [java.net URI]))

(def ld-root
  "For the prototype this item will come from config or be derived from
  it. It should have a trailing slash."
  (URI. "https://example.org/data/"))

;;; ---- KEY CTORS

(defn dataset-series-key [series-slug]
  (str (.getPath ld-root) series-slug))

(defn release-key [series-slug release-slug]
  (str (dataset-series-key series-slug) "/releases/" release-slug))

(defn release-schema-key
  ([{:keys [series-slug release-slug]}]
   (release-schema-key series-slug release-slug))
  ([series-slug release-slug]
   (str (release-key series-slug release-slug) "/schema" )))

(defn revision-key [series-slug release-slug revision-id]
  (str (release-key series-slug release-slug) "/revisions/" revision-id))

;;; --- URI CTORS

(defn dataset-series-uri [series-slug]
  (.resolve ld-root series-slug))

(defn dataset-series-uri* [{:keys [series-slug]}]
  (dataset-series-uri series-slug))

(defn dataset-release-uri [^URI series-uri release-slug]
  (URI. (format "%s/releases/%s" series-uri release-slug)))

(defn dataset-release-uri* [{:keys [series-slug release-slug]}]
  (URI. (format "%s/releases/%s" (dataset-series-uri series-slug) release-slug)))

(defn release-uri-from-slugs [series-slug release-slug]
  (.resolve ld-root (release-key series-slug release-slug)))

(defn release-schema-uri [{:keys [series-slug release-slug]}]
  (.resolve ld-root (release-schema-key series-slug release-slug)))

(defn dataset-revision-uri* [{:keys [series-slug release-slug revision-id]}]
  (URI. (format "%s/releases/%s/revisions/%s" (dataset-series-uri series-slug) release-slug revision-id)))

(defn change-key [series-slug release-slug revision-id change-id]
  (str (revision-key series-slug release-slug revision-id) "/changes/" change-id))

(defn revision-uri [series-slug release-slug revision-id]
  (.resolve ld-root (revision-key series-slug release-slug revision-id)))

(defn change-uri [series-slug release-slug revision-id change-id]
  (assert change-id)
  (.resolve ld-root (change-key series-slug release-slug revision-id change-id)))

(defmulti -resource-uri
  "Returns URI for the given resource."
  (fn [resource _] resource))

(defmethod -resource-uri :dh/DatasetSeries [_ params]
  (dataset-series-uri* params))

(defmethod -resource-uri :dh/Release [_ params]
  (dataset-release-uri* params))

(defmethod -resource-uri :dh/Revision [_ params]
  (dataset-revision-uri* params))

(defmethod -resource-uri :dh/Change [_ {:keys [series-slug release-slug revision-id change-id]}]
  (change-uri series-slug release-slug revision-id change-id))

(defn resource-uri
  "Returns an uri for resource, where resource in
  #{:dh/DatasetsSeries :dh/Release :dh/Revision} and params should
  match the typical path params."
  [resource params]
  (-resource-uri resource params))
