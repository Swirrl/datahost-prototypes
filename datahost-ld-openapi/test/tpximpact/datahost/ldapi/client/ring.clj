(ns tpximpact.datahost.ldapi.client.ring
  (:require
    [clojure.data.json :as json]
    [clojure.set :as set]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.models.resources :as resources]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.ldapi.store.temp-file-store :as tfstore]
    [tpximpact.datahost.system-uris :as su]
    [tpximpact.datahost.time :as time])
  (:import [java.io File]
           [java.net URI]
           [java.lang AutoCloseable]))

(defrecord RingClient [handler clock repo store system-uris]
  AutoCloseable
  (close [_this]
    (.close store)))

(defn system-uris [client]
  (:system-uris client))

(defn repo [client]
  (:repo client))

(defn create-client []
  (let [store (tfstore/create-temp-file-store)
        repo (repo/sail-repo)
        t (time/parse "2023-06-29T10:11:07Z")
        clock (time/manual-clock t)
        system-uris (su/make-system-uris (URI. "http://example.com/"))
        handler (router/handler {:clock clock :triplestore repo :change-store store :system-uris system-uris})]
    (->RingClient handler clock repo store system-uris)))

(defn- response-body->string [body]
  (if (string? body)
    body
    (slurp body)))

(defmulti create-resource (fn [_client _at resource] (::resources/type resource)))

(defmethod create-resource :dh/Change [{:keys [handler clock] :as _client} at change]
  (time/set-now clock at)
  (let [appends-file (File/createTempFile "change" nil)
        data (resources/change-csv change)]
    (try
      (spit appends-file data)
      (let [request {:uri (resources/create-resource-path change)
                     :request-method :post
                     :query-params (merge {"format" "text/csv"}
                                          (set/rename-keys (resources/resource->doc change)
                                                           {"dcterms:title" "title"
                                                            "dcterms:description" "description"}))
                     :headers {"content-type" "text/csv"}
                     :body (clojure.java.io/input-stream (.getBytes data))}
            response (handler request)]
        (if (= 201 (:status response))
          (resources/on-created change response)
          (throw (ex-info "Failed to create change" {:response (update response :body response-body->string)}))))
      (finally
        (.delete appends-file)))))

(defmethod create-resource :default [{:keys [handler clock] :as _client} at resource]
  (time/set-now clock at)
  (let [request {:uri (resources/create-resource-path resource)
                 :request-method (resources/resource-create-method resource)
                 :headers {"content-type" "application/json" "accept" "application/ld+json"}
                 :body (json/write-str (resources/resource->doc resource))}
        response (handler request)]
    (if (contains? #{200 201} (:status response))
      (resources/on-created resource response)
      (throw (ex-info "Failed to create resource" {:resource resource :request request :response response})))))

(defn delete-resource [{:keys [handler clock]} at resource]
  (time/set-now clock at)
  (let [request {:request-method :delete
                 :uri (resources/resource-path resource)}
        {:keys [status] :as response} (handler request)]
    (when (>= status 400)
      (throw (ex-info "Request failed" {:request request :response response})))))

(defmulti get-resource (fn [_client resource] (::resources/type resource)))

(defmethod get-resource :dh/Change [{:keys [handler] :as _client} change]
  (let [request {:request-method :get
                 :uri (resources/resource-path change)
                 :headers {"accept" "text/csv"}}
        {:keys [status body] :as response} (handler request)]
    (when (= 200 status)
      body)))

(defmethod get-resource :default [{:keys [handler] :as client} resource]
  (let [request {:request-method :get
                 :uri (resources/resource-path resource)
                 :headers {"accept" "application/json"}}
        {:keys [status body] :as _response} (handler request)]
    (when (= 200 status)
      (json/read-str body))))
