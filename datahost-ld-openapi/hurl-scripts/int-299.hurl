# Description: test that creates two series with their own releases, revisions 
# and a shared schema. Both contain a change with the same contents - these 
# changes should have the same address within the store. The test deletes one of 
# the series (and therefore its associated change) and checks that the contents 
# of the change in the other series can still be fetched

PUT {{scheme}}://{{host_name}}/data/{{series}}-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Series 1",
    "dcterms:description": "A very simple test"
}

HTTP 201

PUT {{scheme}}://{{host_name}}/data/{{series}}-2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Series 2",
    "dcterms:description": "A very simple test"
}

HTTP 201

PUT {{scheme}}://{{host_name}}/data/{{series}}-1/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

PUT {{scheme}}://{{host_name}}/data/{{series}}-2/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}-1/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-name-age.json;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}-2/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-name-age-2.json;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}-1/release/release-1/revisions
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

POST {{scheme}}://{{host_name}}/data/{{series}}-2/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 1",
    "dcterms:description": "A test revision"
}

HTTP 201
[Captures]
revision2_url: header "Location"

# confirm we can submit data
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}-1
file,common/name-age-01.csv;

HTTP 201

# confirm we can submit data
POST {{scheme}}://{{host_name}}{{revision2_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}-2
file,common/name-age-01.csv;

HTTP 201

DELETE {{scheme}}://{{host_name}}/data/{{series}}-1

HTTP 204

# Verify data for the 'other' release is still there
GET {{scheme}}://{{host_name}}/data/{{series}}-2/release/release-1/revision/1/commit/1
Accept: text/csv
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
```
name,age
bob,25
cindy,29
max,33
```
