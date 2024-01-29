# Description: Basic smoke tests for release data.


GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1-medatata.json

HTTP 404
[Asserts]
body matches /Not found/

PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Test Series",
	"dcterms:description": "A very simple series"
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

