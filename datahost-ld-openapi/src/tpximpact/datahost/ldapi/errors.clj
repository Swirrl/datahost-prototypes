(ns tpximpact.datahost.ldapi.errors
  (:require
    [ring.util.http-status :as status]
    [ring.util.response :as response]
    [reitit.ring.middleware.exception :as exception]))

(defn not-found-response [request]
  {:status 404
   :body (if (some->> (response/get-header request "accept")
                      (re-find #".*application\/(json|ld\+json).*"))
           {"message" "Not found"}
           "Not found")})

;; type hierarchy
(derive ::error ::exception)
(derive ::failure ::exception)
(derive ::validation-failure ::exception)
(derive ::not-found ::exception)
(derive ::input-data-error ::exception)

(defn error-body-base [exception request]
  {:exception (.getClass exception)
   :data (ex-data exception)
   :uri (:uri request)})

(defn default-error-handler [message exception request]
  {:status status/internal-server-error
   :body (assoc (error-body-base exception request)
           :status "error"
           :message message)})

(defn validation-error-handler [exception-info request]
  {:status status/unprocessable-entity
   :body (assoc (error-body-base exception-info request)
                :status "validation-error"
                :message (str "Submitted data is invalid: " (ex-message exception-info)))})

(def exception-middleware
  (exception/create-exception-middleware
    (merge
      exception/default-handlers
      {;; ex-data with :type ::error
       ::error (partial default-error-handler "Error")

       ;; ex-data with ::exception or ::failure
       ::exception (partial default-error-handler "Exception")

       ::validation-failure validation-error-handler

       ::input-data-error validation-error-handler

       ;; example of dispatch by class type
       ;java.lang.ArithmeticException (partial handler "Number Wang")

       ;; override the default handler
       ::exception/default (partial default-error-handler "An error occurred")

       ;; print stack-traces for all exceptions
       ::exception/wrap (fn [handler e request]
                          (.printStackTrace e)
                          (handler e request))})))
