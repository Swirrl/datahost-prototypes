(ns tpximpact.datahost.ldapi.router
  (:require
    [clojure.java.io :as io]
    [com.yetanalytics.flint :as fl]
    [integrant.core :as ig]
    [reitit.dev.pretty :as pretty]
    [reitit.interceptor.sieppari :as sieppari]
    [reitit.openapi :as openapi]
    [reitit.ring :as ring]
    [reitit.ring.coercion :as coercion]
    [reitit.ring.middleware.multipart :as multipart]
    [reitit.ring.middleware.muuntaja :as muuntaja]
    [reitit.ring.middleware.parameters :as parameters]
    [reitit.swagger :as swagger]
    [reitit.swagger-ui :as swagger-ui]
    [tpximpact.datahost.ldapi.native-datastore :as datastore]
    [reitit.coercion.malli :as rcm]
    [muuntaja.core :as m]
    [muuntaja.format.core :as fc]
    [malli.util :as mu]
    [tpximpact.datahost.ldapi.routes.series :as series-routes]
    [tpximpact.datahost.ldapi.routes.release :as release-routes]
    [tpximpact.datahost.ldapi.routes.revision :as revision-routes]
    [tpximpact.datahost.ldapi.errors :as ldapi-errors]
    [ring.middleware.cors :as cors]
    [tpximpact.datahost.ldapi.store.triplestore :as tstore]
    [tpximpact.datahost.ldapi.store.file :as fstore])
  (:import (java.io InputStream InputStreamReader OutputStream)))

(defn decode-str [options]
  (reify
    fc/Decode
    (decode [_ data charset]
      (slurp (InputStreamReader. ^InputStream data ^String charset)))))

(defn encode-str [options]
  (reify
    fc/EncodeToBytes
    (encode-to-bytes [_ data charset]
      (.getBytes data ^String charset))
    fc/EncodeToOutputStream
    (encode-to-output-stream [_ data charset]
      (fn [^OutputStream output-stream]
        (.write output-stream
                (.getBytes data ^String charset))))))

(def csv-format
  (fc/map->Format
    {:name "text/csv"
     :decoder [decode-str]
     :encoder [encode-str]}))

(def muuntaja-custom-instance
  (m/create
    (-> (assoc-in
          m/default-options
          [:formats "text/csv"] csv-format))))

(defn query-example [triplestore request]
    ;; temporary code to facilitate end-to-end service wire up
  (let [qry {:prefixes {:dcat "<http://www.w3.org/ns/dcat#>"
                        :rdfs "<http://www.w3.org/2000/01/rdf-schema#>"}
             :select '[?label ?g]
             :where [[:graph datastore/background-data-graph
                      '[[?datasets a :dcat/Catalog]
                        [?datasets :rdfs/label ?label]]]]}

        results (datastore/eager-query triplestore (fl/format-query qry :pretty? true))]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (-> results first :label)}))

(def leave-keys-alone-muuntaja-coercer
  (m/create
   (-> m/default-options
       (assoc-in [:formats "application/json" :decoder-opts] {:decode-key-fn identity})
       (assoc-in [:formats "application/json" :encoder-opts] {:encode-key-fn identity}))))

(def cors-middleware
  "Defines a CORS middleware for a route"
  {:name ::cors
   :compile (fn [_route _opts]
              (fn [handler]
                ;; apply CORS to each router
                ;; NOTE: in future we might want to make this opt-in for specific routes
                (cors/wrap-cors handler
                                :access-control-allow-origin (constantly true)
                                :access-control-allow-methods [:get :post :put])))})

(defn router [clock triplestore change-store]
  (ring/router
   [["/triplestore-query"
     ;; TODO remove this route when we have real ones using the triplestore
     {:get {:no-doc true
            :handler #(query-example triplestore %)}}]
    ["/openapi.json"
     {:get {:no-doc true
            :openapi {:openapi "3.0.0"
                      :info {:title "Prototype OpenData API"
                             :description (str "Source viewable in GitHub "
                                               "[here](https://github.com/Swirrl/datahost-prototypes/tree/main/datahost-ld-openapi).")
                             :version "0.0.2"}
                      ;; used in /secure APIs below
                      :components {:securitySchemes {"auth" {:type :apiKey
                                                             :in :header
                                                             :name "Example-Api-Key"}}}}
            :handler (openapi/create-openapi-handler)}}]

    ["/data" {:muuntaja leave-keys-alone-muuntaja-coercer
              :tags ["linked data api"]}
     ["/:series-slug"
      {:get (series-routes/get-series-route-config triplestore)
       :put (series-routes/put-series-route-config clock triplestore)}]

     ["/:series-slug/releases/:release-slug"
      {:get (release-routes/get-release-route-config triplestore change-store)
       :put (release-routes/put-release-route-config clock triplestore)}]

     ["/:series-slug/releases/:release-slug/schemas"
      {:get (release-routes/get-release-ld-schema-config triplestore)}]
     ["/:series-slug/releases/:release-slug/schemas/:schema-slug"
      {:post (release-routes/put-release-ld-schema-config clock triplestore)}]

     ["/:series-slug/releases/:release-slug/revisions"
      {:post (revision-routes/post-revision-route-config triplestore)}]
     ["/:series-slug/releases/:release-slug/revisions/:revision-id"
      {:get (revision-routes/get-revision-route-config triplestore change-store)}]
     ["/:series-slug/releases/:release-slug/revisions/:revision-id/changes"
      {:post (revision-routes/post-revision-changes-route-config triplestore change-store)}]
     ["/:series-slug/releases/:release-slug/revisions/:revision-id/changes/:change-id"
      {:get (revision-routes/get-revision-changes-route-config triplestore change-store)}]]]

   {;;:reitit.middleware/transform dev/print-request-diffs ;; pretty diffs
    ;;:validate spec/validate ;; enable spec validation for route data
    ;;:reitit.spec/wrap spell/closed ;; strict top-level validation
    :exception pretty/exception
    :data {:coercion (rcm/create
                      {;; set of keys to include in error messages
                       :error-keys #{#_:type :coercion :in :schema :value :errors :humanized #_:transformed}
                       ;; schema identity function (default: close all map schemas)
                       :compile mu/closed-schema
                       ;; strip-extra-keys (effects only predefined transformers)
                       :strip-extra-keys true
                       ;; add/set default values
                       :default-values true
                       ;; malli options
                       :options nil})
           :muuntaja muuntaja-custom-instance
           :middleware [cors-middleware
                        ;; swagger & openapi
                        swagger/swagger-feature
                        openapi/openapi-feature
                        ;; query-params & form-params
                        parameters/parameters-middleware
                        ;; content-negotiation
                        muuntaja/format-negotiate-middleware
                        ;; encoding response body
                        muuntaja/format-response-middleware
                        ;; exception handling
                        ldapi-errors/exception-middleware
                        ;; decoding request body
                        muuntaja/format-request-middleware
                        ;; coercing response bodies
                        coercion/coerce-response-middleware
                        ;; coercing request parameters
                        coercion/coerce-request-middleware
                        ;; multipart
                        multipart/multipart-middleware]}}))

(defn handler [clock triplestore change-store]
  (ring/ring-handler
    (router clock triplestore change-store)
    (ring/routes
      (swagger-ui/create-swagger-ui-handler
        {:path "/"
         :config {:validatorUrl nil
                  :urls [{:name "swagger", :url "swagger.json"}
                         {:name "openapi", :url "openapi.json"}]
                  :urls.primaryName "openapi"
                  :operationsSorter "alpha"}})
      (ring/create-default-handler))
    {:executor sieppari/executor}))

(defmethod ig/init-key :tpximpact.datahost.ldapi.router/handler
  [_ {:keys [clock triplestore change-store]}]
  (handler clock triplestore change-store))
