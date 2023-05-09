(ns tpximpact.datahost.ldapi.router
  (:require
   [integrant.core :as ig]
   [reitit.dev.pretty :as pretty]
   [reitit.openapi :as openapi]
   [reitit.ring :as ring]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.exception :as exception]
   [reitit.ring.middleware.multipart :as multipart]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.parameters :as parameters]
   [reitit.interceptor.sieppari :as sieppari]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui])
  (:require [reitit.ring :as ring]
            [reitit.coercion.malli]
            [reitit.openapi :as openapi]
            [reitit.ring.malli]
            [reitit.swagger :as swagger]
            [reitit.swagger-ui :as swagger-ui]
            [reitit.ring.coercion :as coercion]
            [reitit.dev.pretty :as pretty]
            [reitit.ring.middleware.muuntaja :as muuntaja]
            [reitit.ring.middleware.exception :as exception]
            [reitit.ring.middleware.multipart :as multipart]
            [reitit.ring.middleware.parameters :as parameters]
    ;       [reitit.ring.middleware.dev :as dev]
    ;       [reitit.ring.spec :as spec]
    ;       [spec-tools.spell :as spell]
            [ring.adapter.jetty :as jetty]
            [muuntaja.core :as m]
            [clojure.java.io :as io]
            [malli.util :as mu]))



(defn router []
  (ring/router
   [["/openapi.json"
     {:get {:no-doc true
            :openapi {:openapi "3.0.0"
                      :info {:title "Prototype OpenData API"
                             :description "blah blah blah. [a link](http://foo.com/)
* bulleted
* list
* here "
                             :version "0.0.1"}
                      ;; used in /secure APIs below
                      :components {:securitySchemes {"auth" {:type :apiKey
                                                             :in :header
                                                             :name "Example-Api-Key"}}}}
            :handler (openapi/create-openapi-handler)}}]

    ["/data" {:tags ["linked data api"]}
     ["/:dataset-series"
      {:get {:summary "Retrieve metadata for an existing dataset-series"
             :description "blah blah blah. [a link](http://foo.com/)
* bulleted
* list
* here"
             :parameters {:path {:dataset-series string?}}
             :responses {200 {:body {:name string?, :size int?}}}
             :handler (fn [{{:keys [dataset-series]} :path-params
                           {:keys [title description]} :query}]
                        ;;(sc.api/spy)
                        {:status 200
                         :body {:name "foo"
                                :size 123}})}
       :put {:summary "Create or update metadata on a dataset-series"

             :parameters {:path {:dataset-series string?}
                          :query [:map
                                  [:title {:title "X parameter"
                                           :description "Description for X parameter"
                                           :optional true} string?]
                                  [:description {:optional true} string?]]}

             :handler (fn [{{:keys [dataset-series]} :parameters}]
                        ;;(sc.api/spy)
                        {:status 200
                         :body {:name "foo"
                                :size 123}})}}]]]

   { ;;:reitit.middleware/transform dev/print-request-diffs ;; pretty diffs
    ;;:validate spec/validate ;; enable spec validation for route data
    ;;:reitit.spec/wrap spell/closed ;; strict top-level validation
    :exception pretty/exception
    :data {:coercion (reitit.coercion.malli/create
                      { ;; set of keys to include in error messages
                       :error-keys #{#_:type :coercion :in :schema :value :errors :humanized #_:transformed}
                       ;; schema identity function (default: close all map schemas)
                       :compile mu/closed-schema
                       ;; strip-extra-keys (effects only predefined transformers)
                       :strip-extra-keys true
                       ;; add/set default values
                       :default-values true
                       ;; malli options
                       :options nil})
           :muuntaja m/instance
           :middleware [ ;; swagger & openapi
                        swagger/swagger-feature
                        openapi/openapi-feature
                        ;; query-params & form-params
                        parameters/parameters-middleware
                        ;; content-negotiation
                        muuntaja/format-negotiate-middleware
                        ;; encoding response body
                        muuntaja/format-response-middleware
                        ;; exception handling
                        exception/exception-middleware
                        ;; decoding request body
                        muuntaja/format-request-middleware
                        ;; coercing response bodys
                        coercion/coerce-response-middleware
                        ;; coercing request parameters
                        coercion/coerce-request-middleware
                        ;; multipart
                        multipart/multipart-middleware]}}))


(defmethod ig/init-key :tpximpact.datahost.ldapi.router/handler
  [_ {:keys [api-route-data] ::ring/keys [opts default-handlers handlers]}]
  (ring/ring-handler
   (router)
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
