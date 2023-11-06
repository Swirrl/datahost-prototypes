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


#Create release
PUT {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201


#Add revision to release and ensure the URI is as expected, the 
#title and desc are correct, and the url auto-increments to 1.
POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
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


POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,simple.json;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,2021.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/corrections
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes corrects
description: change for {{series}}
file,2021-corrections.csv;

HTTP 201

POST {{scheme}}://{{host_name}}{{revision1_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes deletes
description: change for {{series}}
file,2021-deletes.csv;

HTTP 201


GET {{scheme}}://{{host_name}}{{revision1_url}}
Accept: text/csv

HTTP 200