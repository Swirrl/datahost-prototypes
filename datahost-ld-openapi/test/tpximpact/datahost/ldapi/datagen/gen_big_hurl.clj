(ns gen_big_hurl)


(defn generate-post-request [index]
  (str "#CYCLE" (str index) "
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,onemil.csv;
       
HTTP 201
       
POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,onemil.csv;
       
HTTP 201\n"))

(defn generate-hurl []
  (str (apply str (map generate-post-request (range 100)))))

(println (generate-hurl))