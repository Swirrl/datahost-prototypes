(ns tpximpact.datahost.ldapi.secrets
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [integrant.core :as ig])
  (:import (com.google.cloud.secretmanager.v1
            SecretManagerServiceClient SecretVersionName)))

(s/def ::gcloud-project string?)
(s/def ::secret-name string?)
(s/def ::version string?)

(defmethod ig/pre-init-spec ::basic-auth-hash [_]
  (s/keys :req-un [::gcloud-project ::secret-name ::version]))

(defmethod ig/init-key ::basic-auth-hash
  [_ {:keys [gcloud-project secret-name version]}]
  (when-not (Boolean/valueOf (System/getenv "CI"))
    (with-open [client (SecretManagerServiceClient/create)]
      (let [secret-version (SecretVersionName/of gcloud-project secret-name version)
            latest (.accessSecretVersion client secret-version)]
        (-> latest .getPayload .getData .toByteArray (String. "UTF-8"))))))
