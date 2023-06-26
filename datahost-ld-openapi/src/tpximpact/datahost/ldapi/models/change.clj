(ns tpximpact.datahost.ldapi.models.change
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [malli.core :as m]
            [malli.error :as me]
            [tpximpact.datahost.ldapi.schemas.common :refer [registry]]
            [tpximpact.datahost.ldapi.models.shared :as models-shared])
  (:import (java.time ZoneId ZonedDateTime)
           (java.util UUID)))

(def ChangeApiParams [:map
                      [:series-slug :datahost/slug-string]
                      [:release-slug :datahost/slug-string]
                      [:revision-id :int]
                      ;[:description :string]
                      ])

(defn normalise-change [base-entity api-params change-id appends-file-key jsonld-doc]
  (let [{:keys [series-slug release-slug revision-id]} api-params
        _ (assert base-entity "Expected base entity to be set")]
    (when-not (m/validate ChangeApiParams
                          api-params
                          {:registry registry})
      (throw (ex-info "Invalid API parameters"
                      {:type :validation-error
                       :validation-error (-> (m/explain ChangeApiParams
                                                        api-params
                                                        {:registry registry})
                                             (me/humanize))})))

    (let [cleaned-doc (models-shared/merge-params-with-doc api-params jsonld-doc)

          validated-doc (-> (models-shared/validate-id change-id cleaned-doc)
                            (models-shared/validate-context base-entity))

          final-doc (assoc validated-doc
                      "@type" "dh:Change"
                      "@id" change-id
                      "dcterms:issued" (-> (ZonedDateTime/now (ZoneId/of "UTC"))
                                           (.format models-shared/date-formatter))
                      "dh:appends" appends-file-key
                      "dh:appliesToRevision" (str ".." (models-shared/revision-key series-slug release-slug revision-id)))]
      final-doc)))

(def files-root-key "/data/files")

(defn new-file-key [filename]
  (str files-root-key "/" (UUID/randomUUID) "/" filename))

(defn- create-change [base-entity api-params change-id appends-file-key change-doc]
  (log/info "Creating change "
            (str/join "/" [(:series-slug api-params) (:release-slug api-params)
                           (:revision-id api-params) change-id]))
  (normalise-change base-entity api-params change-id appends-file-key change-doc))

(defn insert-change [db api-params change-id change-doc appends-tmp-file]
  (let [{:keys [series-slug release-slug revision-id]} api-params
        revision-key (models-shared/revision-key series-slug release-slug revision-id)
        change-key (models-shared/change-key series-slug release-slug revision-id change-id)
        series-key (models-shared/dataset-series-key series-slug)
        series (get db series-key)
        base-entity (get series "dh:baseEntity")
        appends-file-key (new-file-key (:filename appends-tmp-file))
        normalised-doc (create-change base-entity api-params change-id appends-file-key change-doc)]
    (-> (assoc db change-key normalised-doc
                  ;; File stored in DB. Only for prototype: Will probably ultimately be stored in S3 or similar.
                  appends-file-key (-> appends-tmp-file :tempfile slurp))
        ;; revision also gets the inverse change triple
        (update-in [revision-key "dh:hasChange"]
                   #(conj (vec %) change-key)))))