# Issue: https://github.com/Swirrl/datahost-prototypes/issues/348
#   For consistency dh:hasRevision should always be an array, even if there is only 
#   one revision in the series.
# Description:
#  1. Create a new series.
#  2. Create a new release (rel1) in that series.
#  3. Create a new revision (rev1) in release `rel1`.
#  4. Create a new release (rel2) in the series.
#  5. Create new revisions (rev1 and rev2) in release `rel2`.
#  6. Get the releases metadata, confirm that `dh:hasRevision` for each release is an array.
# Usage:
#  Run locally using hurl:
#  > hurl issue-348.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"

# 1. Create a new series
PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201

# 2. Create a new release `rel1`
PUT {{scheme}}://{{host_name}}/data/{{series}}/release/rel1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple release"
}

HTTP 201

#  3. Create a new revision (rev1) in release `rel1`.
POST {{scheme}}://{{host_name}}/data/{{series}}/release/rel1/revisions
Accept: application/json 
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 1",
    "dcterms:description": "A test revision"
}

HTTP 201

#  4. Create a new release (rel2) in the series.
PUT {{scheme}}://{{host_name}}/data/{{series}}/release/rel2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Another test release",
    "dcterms:description": "Another very simple release"
}

HTTP 201

#  5. Create new revisions (rev1 and rev2) in release `rel2`.
POST {{scheme}}://{{host_name}}/data/{{series}}/release/rel2/revisions
Accept: application/json 
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 1",
    "dcterms:description": "First test revision in rel2"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/rel2/revisions
Accept: application/json 
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 2",
    "dcterms:description": "Second test revision in rel2"
}

HTTP 201

# 6. Get the releases metadata, confirm that `dh:hasRevision` for each release is an array.
GET {{scheme}}://{{host_name}}/data/{{series}}/releases
Accept: application/json 
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.contents" count == 2
jsonpath "$.contents.[0].['dh:hasRevision']" count == 2
jsonpath "$.contents.[1].['dh:hasRevision']" count == 1