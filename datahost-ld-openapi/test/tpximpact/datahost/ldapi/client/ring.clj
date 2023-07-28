(ns tpximpact.datahost.ldapi.client.ring
  (:require
    [clojure.data.json :as json]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.ldapi.store.temp-file-store :as tfstore]
    [tpximpact.datahost.time :as time])
  (:import [java.lang AutoCloseable]))

(defrecord RingClient [handler clock store]
  AutoCloseable
  (close [_this]
    (.close store)))

(defmulti resource-path (fn [resource] (:type resource)))

(defmethod resource-path :series [{:keys [slug]}]
  (str "/data/" slug))

(defmethod resource-path :release [{:keys [slug parent]}]
  (str (resource-path parent) "/releases/" slug))

(defmethod resource-path :schema [{:keys [slug parent]}]
  (str (resource-path parent) "/schemas/" slug))

(defmethod resource-path :revision [revision]
  (:location revision))

(defmethod resource-path :default [resource]
  (throw (ex-info (str "Unsupported resource type: " (:type resource)) {:resource resource})))

(defmulti create-resource-path (fn [resource] (:type resource)))

(defmethod create-resource-path :default [resource]
  (resource-path resource))

(defmethod create-resource-path :revision [{:keys [parent] :as revision}]
  (str (create-resource-path parent) "/revisions"))

(defmulti resource-create-method (fn [resource] (:type resource)))
(defmethod resource-create-method :default [_resource] :put)
(defmethod resource-create-method :schema [_schema] :post)
(defmethod resource-create-method :revision [_revision] :post)

(defn resource->doc [resource]
  (into {} (remove (fn [[k _v]] (keyword? k)) resource)))

(defmulti resource-created (fn [_create-response _doc resource] (:type resource)))
(defmethod resource-created :default [_create-response _doc resource]
  resource)

(defmethod resource-created :revision [create-response _doc revision]
  (assoc revision :location (get-in create-response [:headers "Location"])))

(defn submit-op [{:keys [handler clock] :as client} {:keys [op resource at]}]
  (time/set-now clock at)
  (let [request (case op
                  :delete {:request-method :delete
                           :uri (resource-path resource)}
                  :create {:uri (create-resource-path resource)
                           :request-method (resource-create-method resource)
                           :headers {"content-type" "application/json"}
                           :body (json/write-str (resource->doc resource))}
                  :update {:uri (resource-path resource)
                           :request-method :put
                           :headers {"content-type" "application/json"}
                           :body (json/write-str (resource->doc resource))})
        {:keys [status body] :as response} (handler request)]
    (cond
      (>= status 400)
      (throw (ex-info "Request failed" {:request request :response response}))

      (= :delete op)
      nil

      :else
      (let [doc (json/read-str body)]
        (resource-created response doc resource)))))

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
