(ns tpximpact.datahost.ldapi.router
  (:require
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
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.routes.series :as series-routes]
   [tpximpact.datahost.ldapi.routes.release :as release-routes]
   [tpximpact.datahost.ldapi.routes.revision :as revision-routes]
   [tpximpact.datahost.ldapi.errors :as ldapi-errors]
   [ring.middleware.cors :as cors]))

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

(defn router [triplestore db]
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
       :put (series-routes/put-series-route-config triplestore)}]

     ["/:series-slug/release/:release-slug"
      {:get (release-routes/get-release-route-config db)
       :put (release-routes/put-release-route-config db triplestore)}]

     ["/:series-slug/release/:release-slug/revisions"
      {:post (revision-routes/post-revision-route-config db)}]
     ["/:series-slug/release/:release-slug/revisions/:revision-id"
      {:get (revision-routes/get-revision-route-config db)}]]]

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
           :muuntaja m/instance
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

(defn handler [triplestore db]
  (ring/ring-handler
    (router triplestore db)
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
  [_ {:keys [triplestore db]}]
  (handler triplestore db))
