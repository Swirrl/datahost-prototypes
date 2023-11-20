# Description: basic append delete and correction test

# This Hurl script was created using hurl-gen.py with the following arguments: release_1-1.json revision appends:test-original.csv revision appends:data-appends.csv retractions:data-deletes.csv corrections:data-corrections.csv


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

PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-01.json;

HTTP 201

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


POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,int-014/data-original.csv;

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
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
file,int-014/data-appends.csv;

HTTP 201


POST {{scheme}}://{{host_name}}{{revision2_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes retractions
description: change for {{series}}
file,int-014/data-deletes.csv;

HTTP 201


POST {{scheme}}://{{host_name}}{{revision2_url}}/corrections
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes corrections
description: change for {{series}}
file,int-014/data-corrections.csv;

HTTP 201
