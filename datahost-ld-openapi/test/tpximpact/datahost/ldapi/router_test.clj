(ns tpximpact.datahost.ldapi.router-test
  (:require
    [clojure.java.io :as io]
    [clojure.test :as t]
    [grafter-2.rdf4j.repository :as repo]
    [reitit.ring :as ring]
    [tpximpact.datahost.ldapi.store.file :as fstore]
    [tpximpact.datahost.time :as time]
    [tpximpact.datahost.ldapi.router :as sut]))

(defn- get-test-router
  ([] (get-test-router time/system-clock))
  ([clock]
   (let [triplestore (repo/sail-repo)
         change-store (fstore/->FileChangeStore (io/file "/Users/lee/data/filestore-tmp"))]
     (sut/router {:clock clock :triplestore triplestore :change-store change-store}))))

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
        request {:request-method :put
                 :uri "/data/new-series"
                 :query-string "title=test"
                 :headers {"content-type" "application/ld+json"
                           "origin" origin}
                 :body {"@context" {"dcterms" "http://wat"}
                        "dcterms:title" "Test"
                        "dcterms:description" "Test series"}}
        {:keys [status headers] :as _response} (handler request)]
    (t/is (= 201 status))
    (t/is (= origin (get headers "Access-Control-Allow-Origin")))))
