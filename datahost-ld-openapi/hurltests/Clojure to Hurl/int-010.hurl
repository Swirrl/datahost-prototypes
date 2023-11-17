# Description: This test is for:
# - creating a new revision
# - getting the list of revisions from the release
# - getting the metadata of the revisions from the '{{release_slug}}/revisions'

#example run command: hurl revision_test-create.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series=dummy4

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
[Asserts]
jsonpath "$.['@id']" endsWith "/revisions/1"
jsonpath "$.['dcterms:title']" == "Rev 1"
jsonpath "$.['dcterms:description']" == "A test revision"
jsonpath "$.['dh:appliesToRelease']" endsWith "data/{{series}}/releases/release-1"


#ensure that there is only one revision and it is the correct revision
GET {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.contents" count == 1
jsonpath "$.contents.[0].['dcterms:title']" == "Rev 1"
jsonpath "$.contents.[0].['dcterms:description']" == "A test revision"
jsonpath "$.contents.[0].['@type']" == "dh:Revision"


#Add a second revision and ensure the id auto increments to 2, the URI is 
#correct and the title and description were updated correctly
POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 2",
    "dcterms:description": "Another test revision"
}

HTTP 201
[Captures]
revision2_url: header "Location"
[Asserts]
jsonpath "$.['@id']" endsWith "/revisions/2"
jsonpath "$.['dcterms:title']" == "Rev 2"
jsonpath "$.['dcterms:description']" == "Another test revision"
jsonpath "$.['dh:appliesToRelease']" endsWith "data/{{series}}/releases/release-1"


#Get metadata of all revisions in a release and ensure that two revisions are listed
GET {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.['dh:hasRevision'].[0]" endsWith "/data/{{series}}/releases/release-1/revisions/2"
jsonpath "$.['dh:hasRevision'].[1]" endsWith "/data/{{series}}/releases/release-1/revisions/1"

#ensure that there are now two revisions and they are the correct revisions
GET {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.contents" count == 2
jsonpath "$.contents.[0].['dcterms:title']" == "Rev 2"
jsonpath "$.contents.[1].['dcterms:title']" == "Rev 1"

