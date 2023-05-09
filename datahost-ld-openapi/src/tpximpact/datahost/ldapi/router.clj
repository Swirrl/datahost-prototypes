(ns tpximpact.datahost.ldapi.router
  (:require [reitit.ring :as ring]
            [integrant.core :as ig]
            [reitit.coercion.malli :as mc]
            [ring.middleware.content-type :as content-type]
            [reitit.middleware :as middleware]
            [reitit.ring.coercion :as rrc]
            [reitit.ring.middleware.exception :as exception]
            [reitit.ring.middleware.muuntaja :as muuntaja]
            [reitit.ring.middleware.parameters :as parameters]
            [muuntaja.core :as muu.c]
            [meta-merge.core :as mm]))

(defmethod ig/init-key :tpximpact.datahost.scratch.router/router
  [_ {:keys [api-route-data] ::ring/keys [opts default-handlers handlers]}]
  (ring/ring-handler
    (ring/router api-route-data
                 (mm/meta-merge
                   {:data {:coercion mc/coercion
                           :muuntaja muu.c/instance
                           :middleware [parameters/parameters-middleware ;; query-params & form-params
                                        ;content-type/wrap-content-type
                                        muuntaja/format-middleware
                                        muuntaja/format-negotiate-middleware ;; content-negotiation
                                        muuntaja/format-response-middleware ;; encoding response body
                                        exception/default-handlers
                                        muuntaja/format-request-middleware ;; decoding request body
                                        rrc/coerce-exceptions-middleware
                                        rrc/coerce-request-middleware ;; coercing request parameters
                                        rrc/coerce-response-middleware ;; coercing response bodies
                                        ]}}
                   opts))
    (ring/routes
      (ring/redirect-trailing-slash-handler)
      (ring/create-default-handler default-handlers))))
