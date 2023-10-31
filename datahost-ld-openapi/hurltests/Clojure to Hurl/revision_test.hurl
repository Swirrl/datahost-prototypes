#This test ensures the response matches the type of change, ie one of :dh/ChangeKindAppend
#It also ensures the URL suffix is as expected in the form /revisions/1/changes/1



DELETE {{scheme}}://{{host_name}}/data/{{series}}

HTTP 204


#Create series
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


#Add schema to release
POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,dummyschema.json;

HTTP 201


#Add revision to release
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
[Asserts]
jsonpath "$.['@id']" endsWith "/revisions/1"
jsonpath "$.['dcterms:title']" == "Rev 1"
jsonpath "$.['dcterms:description']" == "A test revision"
jsonpath "$.['dh:appliesToRelease']" endsWith "data/{{series}}/releases/release-1"


#appends to the revision
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
format: text/csv
file,dummies.csv;

HTTP 204


GET {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/schema
