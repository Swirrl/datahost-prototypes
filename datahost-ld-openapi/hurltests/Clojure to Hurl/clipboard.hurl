
#appends to the revision
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,dummies.csv;

HTTP 201
[Asserts]
jsonpath "$.['dh:changeKind']" == ":dh/ChangeKindAppend"
jsonpath "$.['@id']" == "{{series}}/releases/release-1/revisions/1/changes/1"
jsonpath "$.['dcterms:title']" == "changes appends"
jsonpath "$.['dcterms:description']" == "change for {{series}}"

#Add a second revision to release
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

#Correction

POST {{scheme}}://{{host_name}}{{revision2_url}}/corrections
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes corrections
description: change for {{series}}
file,dummiesC.csv;

HTTP 201
[Asserts]
jsonpath "$.['dh:changeKind']" == ":dh/ChangeKindCorrect"
jsonpath "$.['@id']" == "{{series}}/releases/release-1/revisions/2/changes/1"
jsonpath "$.['dcterms:title']" == "changes corrections"
jsonpath "$.['dcterms:description']" == "change for {{series}}"

#Deletion
POST {{scheme}}://{{host_name}}{{revision2_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes retractions
description: change for {{series}}
file,dummiesD.csv;

HTTP 201
[Asserts]
jsonpath "$.['dh:changeKind']" == ":dh/ChangeKindRetract"
jsonpath "$.['@id']" == "{{series}}/releases/release-1/revisions/2/changes/2"
jsonpath "$.['dcterms:title']" == "changes retractions"
jsonpath "$.['dcterms:description']" == "change for {{series}}"










###################SECOND 


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


GET {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.['dh:hasRevision'].[0]" endsWith "/data/{{series}}/releases/release-1/revisions/2"
jsonpath "$.['dh:hasRevision'].[1]" endsWith "/data/{{series}}/releases/release-1/revisions/1"


GET {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.contents" count == 2
jsonpath "$.contents.[0].['dcterms:title']" == "Rev 2"
jsonpath "$.contents.[1].['dcterms:title']" == "Rev 1"




