(ns tpximpact.datahost.ldapi.models.revision
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [malli.core :as m]
    [malli.error :as me]
    [tpximpact.datahost.ldapi.schemas.common :refer [registry]]
    [tpximpact.datahost.ldapi.models.shared :as models-shared]))

(def RevisionApiParams [:map
                        [:series-slug :datahost/slug-string]
                        [:release-slug :datahost/slug-string]
                        [:title {:optional true} :string]
                        [:description {:optional true} :string]])

(defn normalise-revision [base-entity api-params revision-id jsonld-doc]
  (let [{:keys [release-slug]} api-params
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

          final-doc (assoc validated-doc
                      "@type" "dh:Revision"
                      "@id" revision-id
                      "dh:appliesToRelease" (str "../" release-slug))]
      final-doc)))

(defn- create-revision [base-entity api-params revision-id jsonld-doc]
  (log/info "Creating revision "
            (str/join "/" [(:series-slug api-params) (:release-slug api-params) (:revision-id api-params)]))
  (normalise-revision base-entity api-params revision-id jsonld-doc))

(defn insert-revision [db api-params revision-id jsonld-doc]
  (let [{:keys [series-slug release-slug]} api-params
        revision-key (models-shared/revision-key series-slug release-slug revision-id)
        series-key (models-shared/dataset-series-key series-slug)
        series (get db series-key)
        base-entity (get series "dh:baseEntity")]
      (assoc db revision-key (create-revision base-entity api-params revision-id jsonld-doc))))
