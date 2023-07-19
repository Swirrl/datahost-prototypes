(ns tpximpact.datahost.ldapi.files
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn delete-dir [^File dir]
  (doseq [f (.listFiles dir)]
    (cond (.isFile f)
          (.delete f)

          (.isDirectory f)
          (delete-dir f)

          :else
          nil))
  (.delete dir))

(defn create-temp-directory [prefix]
  (let [dir-path (Files/createTempDirectory prefix (make-array FileAttribute 0))]
    (.toFile dir-path)))
