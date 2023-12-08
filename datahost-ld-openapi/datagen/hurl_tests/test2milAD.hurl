# hurl test2milAD.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"


PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"

PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,twomil-schema.json;

HTTP 201



POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 1",
    "dcterms:description": "A test revision"
}

HTTP 201
[Captures]
revision1_url: header "Location"


#CYCLE 1
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 2
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 3
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 4
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 5
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 6
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 7
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 8
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 9
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

#CYCLE 10
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,twomil.csv;

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revision/1
Accept: text/csv
Authorization: {{auth_token}}
HTTP 200