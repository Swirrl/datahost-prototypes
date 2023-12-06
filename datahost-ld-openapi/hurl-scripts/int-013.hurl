#converting the release test from test.tpximpact.datahost.ldapi.models
#This test is for releases
#hurl-scripts % hurl int-013.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"


PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201



PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201
[Asserts]
jsonpath "$['dcterms:title']" == "Test Release"
jsonpath "$['dcterms:description']" == "A very simple Release"
[Captures]
issuedOG: jsonpath "$['dcterms:issued']"
modifiedOG: jsonpath "$['dcterms:modified']"


#test that a modification is successful and the corrent times are updates
PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "updated title",
    "dcterms:description": "updated desription"
}

HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "updated title"
jsonpath "$['dcterms:description']" == "updated desription"
jsonpath "$['dcterms:issued']" == "{{issuedOG}}"
jsonpath "$['dcterms:modified']" != "{{modifiedOG}}"
[Captures]
initial_doc: body

#check that put that doesnt modify doesn't change anything.
PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "updated title",
    "dcterms:description": "updated desription"
}
HTTP 200
[Asserts]
body == "{{initial_doc}}"


#check that a release added to a non existant series returns 404
GET {{scheme}}://{{host_name}}/data/{{series}}2/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404
[Asserts]
body == "{\"message\":\"Not found\"}"

#getting a release that doesnt exists returns 404
GET {{scheme}}://{{host_name}}/data/{{series}}/release/release1231
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404
[Asserts]
body == "{\"message\":\"Not found\"}"

#"Creating a release for a series that is not found fails gracefully"
PUT {{scheme}}://{{host_name}}/data/{{series}}2/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 422
[Asserts]
body == "Series for this release does not exist"

PUT {{scheme}}://{{host_name}}/data/{{series}}2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "A title",
    "dcterms:description": "Description"
}

HTTP 201


#Creating a release for a series that does exist works
PUT {{scheme}}://{{host_name}}/data/{{series}}2/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Example Release",
    "dcterms:description": "Description",
    "dcterms:license": "http://license-link",
    "dh:coverage": "http://some-geo-reference",
    "dh:geographyDefinition": "http://geo-definition",
    "dh:reasonForChange": "Comment about change"
}

HTTP 201
[Asserts]
jsonpath "$['dcterms:title']" == "Example Release"
jsonpath "$['dcterms:description']" == "Description"
jsonpath "$['@id']" == "{{series}}2/release/release-1"
jsonpath "$['@type']" == "dh:Release"
jsonpath "$['dcterms:license']" == "http://license-link"
jsonpath "$['dcat:inSeries']" == "{{scheme}}://{{host_name}}/data/{{series}}2"
jsonpath "$['dh:coverage']" == "http://some-geo-reference"
jsonpath "$['dh:geographyDefinition']" == "http://geo-definition"
jsonpath "$['dh:reasonForChange']" == "Comment about change"
jsonpath "$['@context'].['dh']" == "https://publishmydata.com/def/datahost/"
jsonpath "$['@context'].['dcterms']" == "http://purl.org/dc/terms/"
jsonpath "$['@context'].['rdf']" == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
jsonpath "$['@context'].['rdfs']" == "http://www.w3.org/2000/01/rdf-schema#"
jsonpath "$['@context'].['dcat']" == "http://www.w3.org/ns/dcat#"
jsonpath "$['@context'].['csvw']" == "http://www.w3.org/ns/csvw#"
jsonpath "$['@context'].['appropriate-csvw']" == "https://publishmydata.com/def/appropriate-csvw/"
jsonpath "$['@context'].['@base']" == "{{scheme}}://{{host_name}}/data/"

#Fetching a release that does exist works
GET {{scheme}}://{{host_name}}/data/{{series}}2/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "Example Release"
jsonpath "$['dcterms:description']" == "Description"
jsonpath "$['@id']" == "{{series}}2/release/release-1"
jsonpath "$['@type']" == "dh:Release"
jsonpath "$['dcterms:license']" == "http://license-link"
jsonpath "$['dcat:inSeries']" == "{{scheme}}://{{host_name}}/data/{{series}}2"
jsonpath "$['dh:coverage']" == "http://some-geo-reference"
jsonpath "$['dh:geographyDefinition']" == "http://geo-definition"
jsonpath "$['dh:reasonForChange']" == "Comment about change"
jsonpath "$['@context'].['dh']" == "https://publishmydata.com/def/datahost/"
jsonpath "$['@context'].['dcterms']" == "http://purl.org/dc/terms/"
jsonpath "$['@context'].['rdf']" == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
jsonpath "$['@context'].['rdfs']" == "http://www.w3.org/2000/01/rdf-schema#"
jsonpath "$['@context'].['dcat']" == "http://www.w3.org/ns/dcat#"
jsonpath "$['@context'].['csvw']" == "http://www.w3.org/ns/csvw#"
jsonpath "$['@context'].['appropriate-csvw']" == "https://publishmydata.com/def/appropriate-csvw/"
jsonpath "$['@context'].['@base']" == "{{scheme}}://{{host_name}}/data/"

#Multiple releases can be can be retrieved via the API
PUT {{scheme}}://{{host_name}}/data/{{series}}2/release/release-2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "A Second Release",
    "dcterms:description": "Description 2"
}
HTTP 201
[Asserts]
jsonpath "$['dcterms:title']" == "A Second Release"
jsonpath "$['dcterms:description']" == "Description 2"
[Captures]
Release2Modified: jsonpath "$['dcterms:modified']"

GET {{scheme}}://{{host_name}}/data/{{series}}2/release/release-2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "A Second Release",
    "dcterms:description": "Description 2"
}
HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "A Second Release"
jsonpath "$['dcterms:description']" == "Description 2"


#A release can be updated, query params take precedence


PUT {{scheme}}://{{host_name}}/data/{{series}}2/release/release-2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
[QueryStringParams]
title: A query params update
{
    "dcterms:title": "An update"
}
HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "A query params update"
jsonpath "$['dcterms:modified']" != "{{Release2Modified}}"
[Captures]
Release2Modified2: jsonpath "$['dcterms:modified']"

#"No update when query params same as in existing doc"
PUT {{scheme}}://{{host_name}}/data/{{series}}2/release/release-2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
[QueryStringParams]
title: A query params update

HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "A query params update"
jsonpath "$['dcterms:modified']" == "{{Release2Modified2}}"


#"Fetching csvw metadata for a release that does not exist returns 'not found'"

GET {{scheme}}://{{host_name}}/data/{{series}}3/release/release-1-metadata.json
Accept: text/csv
Content-Type: application/json
Authorization: {{auth_token}}


HTTP 404
[Asserts]
body == "Not found"

PUT {{scheme}}://{{host_name}}/data/{{series}}3
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201

PUT {{scheme}}://{{host_name}}/data/{{series}}3/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}3/release/release-1
Accept: text/csv
Content-Type: application/json
Authorization: {{auth_token}}


HTTP 422