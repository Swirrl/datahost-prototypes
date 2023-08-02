(ns tpximpact.datahost.ldapi.client.ring
  (:require
    [clojure.data.json :as json]
    [clojure.pprint :as pprint]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.ldapi.store.temp-file-store :as tfstore]
    [tpximpact.datahost.time :as time])
  (:import [clojure.lang IDeref]
           (java.io File)
           [java.lang AutoCloseable]))

(defrecord RingClient [handler clock store]
  AutoCloseable
  (close [_this]
    (.close store)))

(defn- get-parent [{:keys [parent] :as resource}]
  (when (nil? parent)
    (println resource))
  (if (instance? IDeref parent)
    (deref parent)
    parent))

(defmulti resource-path (fn [resource] (:type resource)))

(defmethod resource-path :series [{:keys [slug]}]
  (str "/data/" slug))

(defmethod resource-path :release [{:keys [slug] :as release}]
  (str (resource-path (get-parent release)) "/releases/" slug))

(defmethod resource-path :schema [{:keys [slug] :as schema}]
  (str (resource-path (get-parent schema)) "/schemas/" slug))

(defmethod resource-path :revision [revision]
  (:location revision))

(defmethod resource-path :change [change]
  (:location change))

(defmethod resource-path :default [resource]
  (throw (ex-info (str "Unsupported resource type: " (:type resource)) {:resource resource})))

(defmulti create-resource-path (fn [resource] (:type resource)))

(defmethod create-resource-path :default [resource]
  (resource-path resource))

(defmethod create-resource-path :revision [revision]
  (str (create-resource-path (get-parent revision)) "/revisions"))

(defmethod create-resource-path :change [change]
  (str (resource-path (get-parent change)) "/changes"))

(defmulti resource-create-method (fn [resource] (:type resource)))
(defmethod resource-create-method :default [_resource] :put)
(defmethod resource-create-method :schema [_schema] :post)
(defmethod resource-create-method :revision [_revision] :post)

(defmethod resource-create-method :change [_change] :post)

(defn resource->doc [resource]
  (into {} (remove (fn [[k _v]] (keyword? k)) resource)))

(defmulti resource-created (fn [_create-response _doc resource] (:type resource)))
(defmethod resource-created :default [_create-response _doc resource]
  resource)

(defmethod resource-created :revision [create-response _doc revision]
  (assoc revision :location (get-in create-response [:headers "Location"])))

(defmethod resource-created :change [create-response _doc change]
  (assoc change :location (get-in create-response [:headers "Location"])))

(defn- handle-created-response [request {:keys [status body] :as response} resource]
  (if (>= status 400)
    (throw (ex-info "Request failed" {:request request :response response}))
    (let [doc (json/read-str body)]
      (resource-created response doc resource))))
(defmulti create-resource (fn [_client resource] (:type resource)))
(defmethod create-resource :change [{:keys [handler] :as _client} {:keys [data] :as change}]
  (let [appends-file (File/createTempFile "change" nil)]
    (try
      (spit appends-file data)
      (let [append-file-part {:tempfile appends-file
                              :size (.length appends-file)
                              :filename (.getName appends-file)
                              :content-type "text/csv;"}
            request {:uri (create-resource-path change)
                     :request-method :post
                     :multipart-params {:appends append-file-part}
                     :content-type "application/json"
                     :body (json/write-str (resource->doc change))}
            response (handler request)]
        (when (not= 201 (:status response))
          (println "Change fail!")
          (pprint/pprint response)
          (println (slurp (:body response))))
        (handle-created-response request response change))
      (finally
        (.delete appends-file)))))

(defmethod create-resource :default [{:keys [handler] :as client} resource]
  (let [request {:uri (create-resource-path resource)
                 :request-method (resource-create-method resource)
                 :headers {"content-type" "application/json"}
                 :body (json/write-str (resource->doc resource))}
        response (handler request)]
    (handle-created-response request response resource)))

(defn submit-op [{:keys [handler clock] :as client} {:keys [op resource at]}]
  (time/set-now clock at)
  (case op
    :create (create-resource client resource)
    :delete (let [request {:request-method :delete
                           :uri (resource-path resource)}
                  {:keys [status] :as response} (handler request)]
              (when (>= status 400)
                (throw (ex-info "Request failed" {:request request :response response}))))
    :update (let [request {:uri (resource-path resource)
                           :request-method :put
                           :headers {"content-type" "application/json"}
                           :body (json/write-str (resource->doc resource))}
                  response (handler request)]
              ;; TODO: create handler for update responses
              (handle-created-response request response resource))))

(defn create-client []
  (let [store (tfstore/create-temp-file-store)
        repo (repo/sail-repo)
        t (time/parse "2023-06-29T10:11:07Z")
        clock (time/manual-clock t)
        handler (router/handler clock repo store)]
    (->RingClient handler clock store)))

(defn get-resource [{:keys [handler] :as client} resource]
  (let [request {:request-method :get
                 :uri (resource-path resource)
                 :headers {"accept" "application/json"}}
        {:keys [status body] :as _response} (handler request)]
    (when (= 200 status)
      (json/read-str body))))
