(ns tpximpact.datahost.ldapi.models.shared
  (:import
    [java.net URI]
    (java.util UUID)))

(def ld-root
  "For the prototype this item will come from config or be derived from
  it. It should have a trailing slash."
  (URI. "https://example.org/data/"))

;;; ---- KEY CTORS

(defn dataset-series-uri [series-slug]
  (.resolve ld-root series-slug))

(defn dataset-release-uri [^URI series-uri release-slug]
  (URI. (format "%s/releases/%s" series-uri release-slug)))

(defn dataset-series-key [series-slug]
  (str (.getPath ld-root) series-slug))

(defn new-dataset-file-uri [filename]
  (.resolve ld-root (str "files/" (UUID/randomUUID) "/" filename)))

(defn release-key [series-slug release-slug]
  (str (dataset-series-key series-slug) "/releases/" release-slug))

(defn release-uri-from-slugs [series-slug release-slug]
  (.resolve ld-root (release-key series-slug release-slug)))

(defn release-schema-key
  ([{:keys [series-slug release-slug]}]
   (release-schema-key series-slug release-slug))
  ([series-slug release-slug]
   (str (release-key series-slug release-slug) "/schema" )))

(defn release-schema-uri [series-slug release-slug]
  (.resolve ld-root (release-schema-key series-slug release-slug)))

(defn dataset-revision-uri [^URI dataset-release-uri revision-id]
  (URI. (format "%s/revisions/%s" dataset-release-uri revision-id)))

(defn revision-key [series-slug release-slug revision-id]
  (str (release-key series-slug release-slug) "/revisions/" revision-id))

(defn change-key [series-slug release-slug revision-id change-id]
  (str (revision-key series-slug release-slug revision-id) "/changes/" change-id))

(defn revision-uri [series-slug release-slug revision-id]
  (.resolve ld-root (revision-key series-slug release-slug revision-id)))

(defn change-uri [series-slug release-slug revision-id change-id]
  (.resolve ld-root (change-key series-slug release-slug revision-id change-id)))
