PUT http://localhost:3000/data/{{series}}
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Test Dataset",
	"dcterms:description": "A very simple test"
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"

PUT http://localhost:3000/data/{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Test Release",
	"dcterms:description": "A very simple Release"
}

HTTP 201

POST http://localhost:3000/data/{{series}}/releases/release-1/schema
Content-Type: application/json
file,test-schema.json;

HTTP 201

POST http://localhost:3000/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 1",
	"dcterms:description": "A test revision 1"
}

HTTP 201
[Captures]
revision1_url: header "Location"

POST http://localhost:3000{{revision1_url}}/appends
Content-Type: text/csv
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,change1.csv;

HTTP 201

GET http://localhost:3000/data/{{series}}/releases/release-1
Accept: text/csv

HTTP 200

POST http://localhost:3000/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 2",
	"dcterms:description": "A test revision 2 (with retraction)"
}

HTTP 201
[Captures]
revision2_url: header "Location"

POST http://localhost:3000{{revision2_url}}/retractions
Content-Type: text/csv
[QueryStringParams]
title: A retraction
description: retraction for the release
format: text/csv
file,change2.csv;

HTTP 201

GET http://localhost:3000/data/{{series}}/releases/release-1
Accept: text/csv

HTTP 200

POST http://localhost:3000/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 3",
	"dcterms:description": "A test revision 3 (with corrections)"
}

HTTP 201
[Captures]
revision3_url: header "Location"

POST http://localhost:3000{{revision3_url}}/corrections
Content-Type: text/csv
[QueryStringParams]
title: A correction
description: correction for the release
format: text/csv
file,change3.csv;

HTTP 201

GET http://localhost:3000/data/{{series}}/releases/release-1
Accept: text/csv

HTTP 200
[Asserts]
body matches /max,44/
