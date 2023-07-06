(ns tpximpact.datahost.ldapi.models.revision
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [ring.util.io :as ring-io]
   [malli.core :as m]
   [malli.error :as me]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.schemas.common :refer [registry]]
   [tpximpact.datahost.ldapi.models.shared :as models-shared])
  (:import (java.io ByteArrayInputStream)))

(def RevisionApiParams [:map
                        [:series-slug :datahost/slug-string]
                        [:release-slug :datahost/slug-string]
                        [:title {:optional true} :string]
                        [:description {:optional true} :string]])

(defn normalise-revision [base-entity api-params revision-id jsonld-doc]
  (let [{:keys [series-slug release-slug]} api-params
        _ (assert base-entity "Expected base entity to be set")]
    (when-not (m/validate RevisionApiParams
                          api-params
                          {:registry registry})
      (throw (ex-info "Invalid API parameters"
                      {:type :validation-error
                       :validation-error (-> (m/explain RevisionApiParams
                                                        api-params
                                                        {:registry registry})
                                             (me/humanize))})))

    (let [cleaned-doc (models-shared/merge-params-with-doc api-params jsonld-doc)

          validated-doc (-> (models-shared/validate-id revision-id cleaned-doc)
                            (models-shared/validate-context base-entity))

          release-key (models-shared/release-key series-slug release-slug)

          final-doc (assoc validated-doc
                      "@type" "dh:Revision"
                      "@id" revision-id
                      "dh:appliesToRelease" release-key)]
      final-doc)))

(defn string->stream
  ([s] (string->stream s "UTF-8"))
  ([s encoding]
   (-> s
       (.getBytes encoding)
       (ByteArrayInputStream.))))

(defn csv-str->dataset [str]
  (some-> str
          (string->stream)
          (tc/dataset {:file-type :csv})))

(defn revision-appends-file-locations
  "Given a Revision as a hash map, returns appends file locations"
  [db revision]
  (some->> (get revision "dh:hasChange")
           (map #(get @db %))
           (map #(get % "dh:appends"))))

(defn csv-file-locations->dataset [db appends-file-locations]
  (some->> appends-file-locations
           (map #(csv-str->dataset (get @db %)))
           (remove nil?)
           (apply tc/concat)))

(defn write-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
   (fn [out-stream]
     (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn revision->csv-stream [db revision]
  (when-let [merged-datasets (csv-file-locations->dataset db (revision-appends-file-locations db revision))]
    (write-to-outputstream merged-datasets)))

(defn change->csv-stream [db change]
  (when-let [dataset (csv-file-locations->dataset db [(get change "dh:appends")])]
    (write-to-outputstream dataset)))

(defn release->csv-stream [db release]
  (let [revisions (some->> (get release "dh:hasRevision")
                           (map #(get @db %)))
        appends-file-keys (some->> revisions
                                   (map (partial revision-appends-file-locations db))
                                   (flatten))]
    (when-let [merged-datasets (csv-file-locations->dataset db appends-file-keys)]
      (write-to-outputstream merged-datasets))))

(defn- create-revision [base-entity api-params revision-id jsonld-doc]
  (log/info "Creating revision "
            (str/join "/" [(:series-slug api-params) (:release-slug api-params) revision-id]))
  (normalise-revision base-entity api-params revision-id jsonld-doc))

(defn insert-revision [db api-params revision-id jsonld-doc]
  (let [{:keys [series-slug release-slug]} api-params
        release-key (models-shared/release-key series-slug release-slug)
        revision-key (models-shared/revision-key series-slug release-slug revision-id)
        series-key (models-shared/dataset-series-key series-slug)
        series (get db series-key)
        base-entity (get series "dh:baseEntity")]
    (-> (assoc db revision-key (create-revision base-entity api-params revision-id jsonld-doc))
        ;; release also gets the inverse revision triple
        (update-in [release-key "dh:hasRevision"]
                   #(conj (vec %) revision-key)))))
