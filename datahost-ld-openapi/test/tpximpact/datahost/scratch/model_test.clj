(ns tpximpact.datahost.scratch.model-test
  (:require
   [clojure.test :as t]
   [clojure.tools.logging :as log]
   [tpximpact.datahost.scratch.series :as series]))

(def db (atom {}))

(defn update-series [old-series {:keys [api-params jsonld-doc] :as _new-series}]
  (log/info "Updating series " (:series-slug api-params))
  (series/normalise-series api-params jsonld-doc))

(defn create-series [api-params jsonld-doc]
  (log/info "Updating series " (:series-slug api-params))
  (series/normalise-series api-params jsonld-doc))

(defn upsert-series [db {:keys [api-params jsonld-doc] :as new-series}]
  (let [k (str (.getPath series/ld-root) (:series-slug api-params))]
    (if-let [old-series (get db k)]
      (update db k update-series new-series)
      (assoc db k (normalise-series api-params jsonld-doc)))))

(defn normalise-release [{:keys [series-slug release-slug] :as api-params} jsonld-doc]
  (let [series-path (str (.getPath series/ld-root) series-slug)
        series (get @db series-path)
        base-entity (get series "dh:base-entity")
        _ (assert base-entity "Expected base entity to be set")
        context ["https://publishmydata.com/def/datahost/context"
                 {"@base" base-entity}]]

    (-> (series/merge-params-with-doc api-params jsonld-doc)
        (assoc "@context" context
               "@id" release-slug
               "dcat:inSeries" (str "../" series-slug)))))

(defn- update-release [old-release {:keys [api-params jsonld-doc] :as _new-release}]
  (log/info "Updating release " (:series-slug api-params) "/" (:release-slug api-params))
  (normalise-release api-params jsonld-doc))

(defn upsert-release [db {:keys [series-slug release-slug] :as api-params} jsonld-doc]
  (let [release-path (str (.getPath series/ld-root) series-slug "/" release-slug)]
    (if-let [old-release (get db release-path)]
      (update db release-path update-release {:api-params api-params :jsonld-doc jsonld-doc})
      (assoc db release-path (normalise-release api-params jsonld-doc)))))

;; create-series
(swap! db upsert-series {:api-params {:series-slug "my-dataset-series"}})

;; upsert release
;;(swap! db normalise-release {:series-slug "my-dataset-series" :release-slug "2018"} {})

(swap! db upsert-release {:series-slug "my-dataset-series" :release-slug "2018"} {"dcterms:title" "my release"})


(defn add-schema [release schema]
  ;; TODO
  )

(def example-changesets [{:description "first changeset"
                          :commits [{:append [["male" "manchester" 3]]
                                     :description "add obs"}]}
                         {:description "second changeset"
                          :commits [{:append [["female" "manchester" 4]]
                                     :description "add obs"}]}
                         {:description "second changeset"
                          :commits [{:delete [["male" "manchester" 3]]
                                     :append [["male" "manchester" 4]]
                                     :description "correct incorrect observation"}]}
                         ])

(def equivalent-changes-in-one-changeset [{:description "Coin the first release"
                                           :commits [{:append [["male" "manchester" 3]]
                                                      :description "add obs"}
                                                     {:append [["female" "manchester" 4]]
                                                      :description "add obs"}
                                                     {:delete [["male" "manchester" 3]]
                                                      :append [["male" "manchester" 4]]
                                                      :description "correct incorrect observation"}]}

                                          ])

(defn add-changeset [release {:keys [description append delete]}])
