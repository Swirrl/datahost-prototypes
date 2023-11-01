#Create a series and make some appends and deletes

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

PUT {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,simple.json;

HTTP 201

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


POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,2019.csv;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 2",
    "dcterms:description": "A test revision"
}

HTTP 201
[Captures]
revision2_url: header "Location"

POST {{scheme}}://{{host_name}}{{revision2_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,2020.csv;

HTTP 201


#create a different series with a revision and no appends

PUT {{scheme}}://{{host_name}}/data/different{{series}}
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

PUT {{scheme}}://{{host_name}}/data/different{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201


POST {{scheme}}://{{host_name}}/data/different{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 1",
    "dcterms:description": "A test revision"
}

HTTP 201

#make a json GET to the new revision that should have no changes yet
#but you will see all revisions from all other series that have changes.

GET {{scheme}}://{{host_name}}/data/different{{series}}/releases/release-1/revisions/1
Accept: application/json

HTTP 200

