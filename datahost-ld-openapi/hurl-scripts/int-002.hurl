# Description: This test creates three revisions, 1 commit in each (1st rev + append, 2nd rev + retractions, 
# 3rd rev + corrections). We check the CSV contents of the release at the end.

PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
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
{
	"dcterms:title": "Test Release",
	"dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
file,common/schema-name-age.json;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 1",
	"dcterms:description": "A test revision 1"
}

HTTP 201
[Captures]
revision1_url: header "Location"

POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,common/name-age-01.csv;

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1.csv

HTTP 302

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 2",
	"dcterms:description": "A test revision 2 (with retraction)"
}

HTTP 201
[Captures]
revision2_url: header "Location"

POST {{scheme}}://{{host_name}}{{revision2_url}}/retractions
Content-Type: text/csv
[QueryStringParams]
title: A retraction
description: retraction for the release
format: text/csv
file,common/name-age-02.csv;

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1.csv

HTTP 302

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 3",
	"dcterms:description": "A test revision 3 (with corrections)"
}

HTTP 201
[Captures]
revision3_url: header "Location"

POST {{scheme}}://{{host_name}}{{revision3_url}}/corrections
Content-Type: text/csv
[QueryStringParams]
title: A correction
description: correction for the release
format: text/csv
file,common/name-age-03.csv;

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1.csv

HTTP 302
[Captures]
final_location: header "Location"

GET {{scheme}}://{{host_name}}{{final_location}}
Accept: text/csv

HTTP 200
[Asserts]
body matches /max,44/


GET {{scheme}}://{{host_name}}/doc/{{series}}/release/release-1.csv-metadata.json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.['@context']" == "http://www.w3.org/ns/csvw"
jsonpath "$.['http://purl.org/dc/terms/title']" == "Fun schema"
jsonpath "$.['tableSchema'].columns" count == 2
jsonpath "$.['url']" == "/doc/{{series}}/release/release-1.csv"
