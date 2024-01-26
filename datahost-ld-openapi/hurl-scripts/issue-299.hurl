# Description: Deleting a series with associated schema worked incorrectly and did not enrirely remove the schema. 
# Future schemas could be polluted with the old shcemas.

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
PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201


#Add schema to release
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-name-age.json;

HTTP 201

#Add revision to release
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

#confirm the schema accepts the data
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,common/name-age-01.csv;

HTTP 201


GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: text/csv
Accept: text/csv

HTTP 200
[Asserts]
jsonpath "$.['dh:columns'].[0].['csvw:name']" == "name"
 


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
PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201


#Add schema to release
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-qualifications-by-area.json;

HTTP 201


#if bug present: new schema is enmeshed with schema from previously deleted series
#eg csvw:titles":["Statistical Geography","maxage"] 
#where the titles are from different schemas
#this test should return a single string, if the bug is present it returns a list
GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: text/csv
Accept: text/csv

HTTP 200
[Asserts]
jsonpath "$.['dh:columns'].[0].['csvw:name']" == "Measure type"

#Add revision to release
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

#confirm the new schema accepts the data
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,common/2019.csv;

HTTP 201