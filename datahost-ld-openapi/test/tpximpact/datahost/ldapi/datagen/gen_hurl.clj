(ns tpximpact.datahost.ldapi.datagen.gen-hurl)

(defn generate-boilerplate-start [filename test]
  (str "# hurl "(str "issue-310-" filename "-" test)".hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token=\"string\" --variable series=\"dummy$(date +%s)\"


PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    \"dcterms:title\": \"Load test\",
    \"dcterms:description\": \"Testing max load\"
}

HTTP 201
[Captures]
dataset: jsonpath \"$['dh:baseEntity']\"

PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    \"dcterms:title\": \"Test Release\",
    \"dcterms:description\": \"A very simple Release\"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,schema-"(str filename) ".json;

HTTP 201



POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    \"dcterms:title\": \"Rev 1\",
    \"dcterms:description\": \"A test revision\"
}

HTTP 201
[Captures]
revision1_url: header \"Location\"\n"))


(defn generate-append [filename]
  (str 
"\nPOST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file," (str filename) ".csv;
       
HTTP 201\n"))

(defn append-delete [index filename]
  (str "\n#CYCLE " (str (+ index 1)) "
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file," (str filename) ".csv;
       
HTTP 201
       
POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes deletes
description: change for {{series}}
file," (str filename) ".csv;
       
HTTP 201\n\n"))

(defn generate-AD [repeat filename]
  (apply str (map #(append-delete % filename) (range repeat))))


(defn consecutive-appends [index filename]
  (str "\n#APPEND NUMBER " (str (+ index 1)) "
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: consecutive appends
description: change for {{series}}
file," (str filename index) ".csv;
       
HTTP 201\n\n"))

(defn generate-consecutive-appends [repeat filename]
  (apply str (map #(consecutive-appends % filename) (range repeat))))

(defn generate-boilerplate-end []
  (str "GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revision/1
Accept: text/csv
Authorization: {{auth_token}}
HTTP 200"))


(defn write-to-file [content path filename test]
  (spit (str path "issue-310-" filename "-" test ".hurl") content :append true))

(defn gen-hurl [path filename test repeats]
  (write-to-file (generate-boilerplate-start filename test) path filename test)
  (case test
    "A" (write-to-file (generate-append filename) path filename test)
    "AD" (write-to-file (generate-AD repeats filename) path filename test)
    "CA" (write-to-file (generate-consecutive-appends repeats filename) path filename test))
  (write-to-file (generate-boilerplate-end) path filename test))
