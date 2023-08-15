(ns tpximpact.datahost.ldapi.secrets
  (:import [com.google.cloud.secretmanager.v1
            ProjectName SecretName SecretVersionName SecretManagerServiceClient])
  (:require [integrant.core :as ig]))

(defmethod ig/init-key ::basic-auth-hash
  [_ {:keys [gcloud-project secret-name version]}]
  (with-open [client (SecretManagerServiceClient/create)]
    (let [secret-version (SecretVersionName/of gcloud-project secret-name version)
          latest (.accessSecretVersion client secret-version)]
      (-> latest .getPayload .getData .toByteArray (String. "UTF-8")))))
