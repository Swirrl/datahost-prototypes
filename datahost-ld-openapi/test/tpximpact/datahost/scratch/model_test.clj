(ns tpximpact.datahost.scratch.model-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [grafter-2.rdf4j.io :as gio]
   [grafter.matcha.alpha :as matcha]
   [grafter.vocabularies.core :refer [prefixer]]
   [grafter.vocabularies.dcat :refer [dcat]]
   [grafter.vocabularies.dcterms :refer [dcterms:title]]
   [tpximpact.datahost.ldapi.util.rdf :as util.rdf]
   [tpximpact.datahost.ldapi.models.release :as release]
   [tpximpact.datahost.ldapi.models.series :as series]
   [tpximpact.datahost.ldapi.models.shared :as models.shared])
  (:import
    [java.net URI]
    (java.util UUID)))

(deftest ontology-parses
  (is (< 0 (count (gio/statements (io/file "./doc/datahost-ontology.ttl"))))))

(defn db->matcha [db]
  (->> db
       vals
       (mapcat util.rdf/ednld->rdf)
       (matcha/index-triples)))

(def dh (prefixer "https://publishmydata.com/def/datahost/"))
(def dh:baseEntity (dh "baseEntity"))
(def dcat:inSeries (dcat "inSeries"))


(deftest loading-data-workflow-with-rdf

  ;; NOTE this is a stateful test with accreting data

  (let [timestamp (fn [] (java.time.ZonedDateTime/now (java.time.ZoneId/of "UTC")))
        new-series-slug (str "new-series-" (UUID/randomUUID))
        new-series-path (str "/data/" new-series-slug)
        example:my-dataset-series (URI. (str "https://example.org" new-series-path))
        example:my-release (URI. (str "https://example.org" new-series-path "/2018"))
        db (atom {})] ;; an empty database
    (testing "Constructing the series"
      ;; first make a series
      (swap! db series/upsert-series {:series-slug new-series-slug
                                      :title "My series"
                                      :op/timestamp (timestamp)
                                      :op.upsert/keys {:series (models.shared/dataset-series-key new-series-slug)}} nil)

      (is (matcha/ask [[example:my-dataset-series dh:baseEntity ?o]] (db->matcha @db)))
      (is (matcha/ask [[example:my-dataset-series dcterms:title "My series"]] (db->matcha @db)))

      (testing "idempotent - upserting same request again is equivalent to inserting once"
        (let [start-state @db
              end-state (swap! db series/upsert-series
                               {:series-slug new-series-slug
                                :title "My series"
                                :op/timestamp (timestamp)
                                :op.upsert/keys {:series (models.shared/dataset-series-key new-series-slug)}}
                               nil)]
          (is (= start-state end-state)))))

    (testing "Constructing a release"
      (swap! db release/upsert-release
	          {:series-slug new-series-slug
               :release-slug "2018"
               :op/timestamp (timestamp)
               :op.upsert/keys {:series (models.shared/dataset-series-key new-series-slug)
                                :release (models.shared/release-key new-series-slug "2018")}}
              {"dcterms:title" "2018"})

      (is (matcha/ask [[example:my-release ?p ?o]] (db->matcha @db)))
      (is (matcha/ask [[example:my-release dcterms:title "2018"]] (db->matcha @db)))

      (testing "idempotent - upserting same request again is equivalent to inserting once"
        (let [start-state @db
              end-state (swap! db release/upsert-release
                               {:series-slug new-series-slug
                                :release-slug "2018"
                                :op/timestamp (timestamp)
                                :op.upsert/keys {:series (models.shared/dataset-series-key new-series-slug)
                                                 :release (models.shared/release-key new-series-slug "2018")}}
                               {"dcterms:title" "2018"})
              sanitise (fn [m] (dissoc m "dcterms:issued" "dcterms:modified"))]
          (is (= (-> start-state
                     (update-in [new-series-path] sanitise)
                     (update-in [(str new-series-path "/releases/2018")] sanitise))
                 (-> end-state
                     (update-in [new-series-path] sanitise)
                     (update-in [(str new-series-path "/releases/2018")] sanitise))))))

      (testing "RDF graph joins up"
        (let [mdb (db->matcha @db)]
          (is (matcha/ask [[example:my-release dcat:inSeries example:my-dataset-series]]
                          mdb))))

      (testing "TODO inverse triples see issue: https://github.com/Swirrl/datahost-prototypes/issues/54"))))
