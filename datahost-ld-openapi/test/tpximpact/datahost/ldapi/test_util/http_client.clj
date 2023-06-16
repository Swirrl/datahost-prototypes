(ns tpximpact.datahost.ldapi.test-util.http-client
  "Conveniencve http-client relying on `clj-http.client` library."
  (:require [clj-http.client :as http]))

(defn- http-request
  [method-fn base-url path & args]
  (assert (not (.endsWith base-url "/")))
  (apply method-fn (str base-url path) args))

(defn make-client
  "Returns a map `{:keys [GET PUT POST DELETE HEAD]}` where each value
  is a function accepting the path string and remaining arguments.

  Example
  ```clj
  (let [{put :PUT} (make-client {:port 8080})]
    (put \"/data/series/my-series\"))
  ```"
  [{:keys [port]}]
  (let [base-url (format "http://localhost:%s" port)]
    {:GET (partial http-request http/get base-url)
     :PUT (partial http-request http/put base-url)
     :POST (partial http-request http/post base-url)
     :DELETE (partial http-request http/delete base-url)
     :HEAD (partial http-request http/head base-url)}))
