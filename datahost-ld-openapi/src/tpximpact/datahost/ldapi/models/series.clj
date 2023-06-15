(ns tpximpact.datahost.ldapi.models.series
  (:require
   [clojure.tools.logging :as log]
   [malli.core :as m]
   [malli.error :as me]
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.models.shared :as models-shared])
  (:import
   [java.time ZonedDateTime]))

(def SeriesPathParams
  (m/schema
   [:map
    [:series-slug :datahost/slug-string]]
   {:registry models-shared/registry}))

(def SeriesQueryParams
  (m/schema
   [:map
    [:title {:optional true} :string]
    [:description {:optional true} :string]]))

(def SeriesApiParams
  (mu/merge
   SeriesPathParams
   SeriesQueryParams))

(def ^:private date-formatter
  java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME)

(defn- validate-issued-unchanged
  "Returns a boolean."
  [old-doc new-doc]
  (let [old-issued (get old-doc "dcterms:issued")
        new-issued (get new-doc "dcterms:issued")]
    (or (nil? old-issued)
        (nil? new-issued)
        (= old-issued (get new-doc "dcterms:issued")))))

(defn- validate-modified-changed
  [old-doc new-doc]
  (let [old-modified (get old-doc "dcterms:modified")
        new-modified (get new-doc "dcterms:modified")]
    (or (nil? old-modified)
        (nil? new-modified)
        (not= old-modified (get new-doc "dcterms:modified")))))

(defmulti -issued+modified-dates
  "Adjusts the 'dcterms:issued' and 'dcterms:modified' of the document.
  Ensures that the new document does not modify the issue date.

  Behaviour differs when issuing a new seris and when modifying an
  existing one."
  (fn [api-params old-doc new-doc]
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn normalise-series
  "Takes api params and an optional json-ld document of metadata, and
  returns a normalised EDN form of the JSON-LD, with the API
  parameters applied, validated and some structures like the context
  normalised."
  ([api-params]
   (normalise-series api-params nil))
  ([{:keys [series-slug] :as api-params} jsonld-doc]
   (when-not (m/validate SeriesApiParams
                         api-params
                         {:registry models-shared/registry})
     (throw (ex-info "Invalid API parameters"
                     {:type :validation-error
                      :validation-error (-> (m/explain SeriesApiParams
                                                       api-params
                                                       {:registry models-shared/registry})
                                            (me/humanize))})))
   (let [cleaned-doc (models-shared/merge-params-with-doc api-params jsonld-doc)
         validated-doc (-> (models-shared/validate-id series-slug cleaned-doc)
                           (models-shared/validate-context (str models-shared/ld-root)))

         final-doc (assoc validated-doc
                          ;; add any managed params
                          "@type" "dh:DatasetSeries"
                          "@id" series-slug
                          ;; coin base-entity to serve as the @base for nested resources
                          "dh:baseEntity" (str models-shared/ld-root series-slug "/"))]
     final-doc)))

(defn- update-series [old-series api-params jsonld-doc]
  ;; we don't want the client to overwrite the 'issued' value
  {:pre [(validate-issued-unchanged old-series jsonld-doc)]
   :post [(some? (get % "dcterms:issued"))
          (some? (get % "dcterms:modified"))]}
  (log/info "Updating series " (:series-slug api-params))
  (->> jsonld-doc
       (normalise-series api-params)
       (issued+modified-dates api-params old-series)))

(defn- create-series [api-params jsonld-doc]
  (log/info "Creating series " (:series-slug api-params))
  (->> jsonld-doc
       (normalise-series api-params)
       (issued+modified-dates api-params nil)))

(def ^:private api-query-params-keys (m/explicit-keys SeriesQueryParams))

(def UpsertArgs
  (let [db-schema [:map {}]
        api-params-schema (m/schema [:map
                                     [:op/timestamp :datahost/timestamp]]
                                    {:registry models-shared/registry})
        input-jsonld-doc-schema [:maybe [:map {}]]]
    [:catn
     [:db db-schema]
     [:api-params api-params-schema]
     [:jsonld-doc input-jsonld-doc-schema]]))

(def upsert-args-valid? (m/validator UpsertArgs))

(defn upsert-series
  "Takes a derefenced db state map with the shape {path jsonld} and
  upserts a new dataset series into it.

  Intended usage is via swap!

  ```
  (swap! db upsert-series {:series-slug \"my-dataset-series\"} {})
  ```

  An upsert will insert a new series if it isn't there already.

  If the series exists already it will replace the majority of the
  fields with those newly supplied, but may ignore or manage some
  fields specially on update.

  For example a `dcterms:issued` time should not change after a
  document is updated."
  ([db api-params jsonld-doc]
   {:pre [(upsert-args-valid? [db api-params jsonld-doc])]
    :post [(validate-issued-unchanged jsonld-doc %)
           (validate-modified-changed jsonld-doc %)]}
   (let [series-key (models-shared/dataset-series-key (:series-slug api-params))
         old-series (get db series-key)]
     (cond
       (and old-series
            (nil? jsonld-doc)
            ;; we don't want an update if the query-params' values are
            ;; the same as the ones in the document already
            (let [query-changes (select-keys api-params api-query-params-keys)]
              (or (empty? query-changes)
                  (let [renamed (models-shared/rename-query-params-to-common-keys query-changes)]
                    (= renamed (select-keys old-series (keys renamed)))))))
       db                               ;NOOP

       old-series
       (update db series-key update-series
               api-params (or jsonld-doc old-series))

       :else
       (assoc db series-key (create-series api-params jsonld-doc))))))
