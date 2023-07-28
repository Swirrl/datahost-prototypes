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

(defmethod resource-path :default [resource]
  (throw (ex-info (str "Unsupported resource type: " (:type resource)) {:resource resource})))

(defn submit-op [{:keys [handler clock] :as client} {:keys [op resource at]}]
  (time/set-now clock at)
  (let [request (case op
                  :delete {:request-method :delete
                           :uri (resource-path resource)}
                  (let [body (into {} (remove (fn [[k _v]] (keyword? k)) resource))]
                    {:uri (resource-path resource)
                     :request-method :put
                     :headers {"content-type" "application/json"}
                     :body (json/write-str body)}))
        {:keys [status body] :as response} (handler request)]
    (cond
      (>= status 400)
      (throw (ex-info "Request failed" {:request request :response response}))

      (= :delete op)
      nil

      :else
      (json/read-str body))))

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

(defn get-series [{:keys [handler]} series-slug]
  (let [request {:request-method :get
                 :uri (str "/data/" series-slug)
                 :headers {"accept" "application/json"}}
        {:keys [status body] :as response} (handler request)]
    (when (= 200 status)
      (json/read-str body))))