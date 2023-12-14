(ns tpximpact.datahost.ldapi.router-test
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.test :as t]
   [grafter-2.rdf4j.repository :as repo]
   [reitit.ring :as ring]
   [tpximpact.datahost.ldapi.store.file :as fstore]
   [tpximpact.datahost.system-uris :as su]
   [tpximpact.datahost.time :as time]
   [tpximpact.datahost.ldapi.router :as sut]
   [tpximpact.test-helpers :as th])
  (:import (java.net URI)))

(defn- get-test-router
  ([] (get-test-router {}))
  ([override-opts]
   (let [triplestore (repo/sail-repo)
         rdf-base-uri (URI. "https://example.org/data/")
         system-uris (su/make-system-uris rdf-base-uri)
         change-store (fstore/->FileChangeStore (io/file "/Users/lee/data/filestore-tmp"))]
     (sut/router (merge {:clock time/system-clock :triplestore triplestore :change-store change-store :system-uris system-uris}
                        override-opts)))))

(t/deftest cors-preflight-request-test
  (let [router (get-test-router)
        handler (ring/ring-handler router)
        origin "http://localhost:3000"
        request {:request-method :options
                 :uri "/data/new-series"
                 :headers {"access-control-request-headers" "content-type"
                           "access-control-request-method" "PUT"
                           "origin" origin}}
        {:keys [status headers] :as _response} (handler request)]
    (t/is (= 200 status))
    (t/is (= origin (get headers "Access-Control-Allow-Origin")))))

(t/deftest cors-request-test
  (let [router (get-test-router)
        handler (ring/ring-handler router)
        origin "http://localhost:3000"
        request (-> (th/jsonld-body-request
                     {"@context" {"dcterms" "http://wat"}
                      "dcterms:title" "Test"
                      "dcterms:description" "Test series"})
                    (assoc :request-method :put
                           :uri "/data/new-series"
                           :query-string "title=test")
                    (assoc-in [:headers "origin"] origin))
        {:keys [status headers] :as _response} (handler request)]
    (t/is (= 201 status))
    (t/is (= origin (get headers "Access-Control-Allow-Origin")))))

(t/deftest csvm-well-known-request-test
  (let [router (get-test-router)
        handler (ring/ring-handler router)
        request {:request-method :get :uri "/.well-known/csvm"}
        {:keys [status body]} (handler request)]
    (t/is (= 200 status))
    (t/is (= "{+url}-metadata.json\nmetadata.json" body))))

(t/deftest base-path-request-test
  (th/with-system {{:keys [GET PUT]} :tpximpact.datahost.ldapi.test/http-client :as sys}
                  (th/start-system ["ldapi/base-system.edn"
                                    "test-system.edn"
                                    "test-system-base-path.edn"
                                    "ldapi/env.edn"])

                  (tpximpact.db-cleaner/clean-db sys)
    (let [{:keys [status headers] :as _response} (PUT "/camino-base/data/new-series-again"
                                                      {:content-type :json
                                                       :body (json/write-str {"@context" {"dcterms" "http://foop.com"}
                                                                              "dcterms:title" "Test base path"
                                                                              "dcterms:description" "Test base path series"})})]
      (t/is (= 201 status)))))
