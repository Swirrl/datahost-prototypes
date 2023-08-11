(ns tpximpact.datahost.ldapi.files
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [org.apache.commons.io FileUtils]))

(defn delete-dir [^File dir]
  (FileUtils/deleteDirectory dir))

(defn create-temp-directory [prefix]
  (let [dir-path (Files/createTempDirectory prefix (make-array FileAttribute 0))]
    (.toFile dir-path)))
