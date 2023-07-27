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

(defn submit-op [{:keys [handler clock] :as client} {:keys [op resource at]}]
  (time/set-now clock at)
  (let [request (case op
                  :delete {:request-method :delete
                           :uri (str "/data/" (:slug resource))}
                  (let [body (into {} (remove (fn [[k _v]] (keyword? k)) resource))]
                    {:uri (str "/data/" (:slug resource))
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

