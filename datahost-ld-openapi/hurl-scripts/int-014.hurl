#series test, tests creation modification and deletion of series
#run using: hurl int-014.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable date="$(date +"%Y-%m-%dT%H:%M:%S")" --variable series="dummy$(date +%s)"

PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "A title",
    "dcterms:description": "Description",
    "rdfs:comment": "Comment",
    "dcterms:publisher": "http://publisher-uri",
    "dcterms:license": "http://uri-of-a-licence.org",
    "dcat:theme": "http://theme-uri",
    "dcat:keywords": ["keyword1", "keyword2"],
    "dh:nextUpdate": "2024-06-30",
    "dh:relatedLinks": ["http://related-1", "http://related-2"],
    "dh:contactName": "Rob Chambers",
    "dh:contactEmail": "heyrob@example.com",
    "dh:contactPhone": "123234234234"
}

HTTP 201
[Asserts]
jsonpath "$['dcterms:title']" == "A title"
jsonpath "$['dcterms:description']" == "Description"
jsonpath "$['rdfs:comment']" == "Comment"
jsonpath "$['dcterms:publisher']" == "http://publisher-uri"
jsonpath "$['dcterms:license']" == "http://uri-of-a-licence.org"
jsonpath "$['dcat:theme']" == "http://theme-uri"
jsonpath "$['dcat:keywords'].[1]" == "keyword1"
jsonpath "$['dcat:keywords'].[0]" == "keyword2"
jsonpath "$['dh:nextUpdate']" == "2024-06-30"
jsonpath "$['dh:relatedLinks'].[0]" == "http://related-1"
jsonpath "$['dh:relatedLinks'].[1]" == "http://related-2"
jsonpath "$['dh:contactName']" == "Rob Chambers"
jsonpath "$['dh:contactEmail']" == "heyrob@example.com"
jsonpath "$['dh:contactPhone']" == "123234234234"


GET {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "A title"
jsonpath "$['dcterms:description']" == "Description"
jsonpath "$['rdfs:comment']" == "Comment"
jsonpath "$['dcterms:publisher']" == "http://publisher-uri"
jsonpath "$['dcterms:license']" == "http://uri-of-a-licence.org"
jsonpath "$['dcat:theme']" == "http://theme-uri"
jsonpath "$['dcat:keywords'].[1]" == "keyword1"
jsonpath "$['dcat:keywords'].[0]" == "keyword2"
jsonpath "$['dh:nextUpdate']" == "2024-06-30"
jsonpath "$['dh:relatedLinks'].[0]" == "http://related-1"
jsonpath "$['dh:relatedLinks'].[1]" == "http://related-2"
jsonpath "$['dh:contactName']" == "Rob Chambers"
jsonpath "$['dh:contactEmail']" == "heyrob@example.com"
jsonpath "$['dh:contactPhone']" == "123234234234"


#Not really sure what this test is trying to do because the clojure test includes a description but says?
#It's testing "Should create series without optional dcterms:description"
PUT {{scheme}}://{{host_name}}/data/{{series}}2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Another title",
    "dcterms:description": "Foo"
}

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200


#test that a modification is successful and the corrent times are updates
PUT {{scheme}}://{{host_name}}/data/{{series}}3
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test series",
    "dcterms:description": "A very simple series"
}

HTTP 201
[Asserts]
jsonpath "$['dcterms:title']" == "Test series"
jsonpath "$['dcterms:description']" == "A very simple series"
[Captures]
issuedOG: jsonpath "$['dcterms:issued']"
modifiedOG: jsonpath "$['dcterms:modified']"


#test that a modification is successful and the corrent times are updates
PUT {{scheme}}://{{host_name}}/data/{{series}}3
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
PUT {{scheme}}://{{host_name}}/data/{{series}}3
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
GET {{scheme}}://{{host_name}}/data/{{series}}4
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404
[Asserts]
body == "{\"message\":\"Not found\"}"

#A series can be created
PUT {{scheme}}://{{host_name}}/data/{{series}}4
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "A title",
    "dcterms:description": "Description"
}

HTTP 201
[Asserts]
jsonpath "$['dcterms:title']" == "A title"
jsonpath "$['dcterms:description']" == "Description"
jsonpath "$['@id']" == "{{series}}4"
jsonpath "$['@type']" == "dh:DatasetSeries"
jsonpath "$['@context'].['dh']" == "https://publishmydata.com/def/datahost/"
jsonpath "$['@context'].['dcterms']" == "http://purl.org/dc/terms/"
jsonpath "$['@context'].['rdf']" == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
jsonpath "$['@context'].['rdfs']" == "http://www.w3.org/2000/01/rdf-schema#"
jsonpath "$['@context'].['dcat']" == "http://www.w3.org/ns/dcat#"
jsonpath "$['@context'].['csvw']" == "http://www.w3.org/ns/csvw#"
jsonpath "$['@context'].['appropriate-csvw']" == "https://publishmydata.com/def/appropriate-csvw/"
jsonpath "$['@context'].['@base']" == "{{expected_scheme}}://{{expected_uri_root}}/data/"

#And can be retrived
GET {{scheme}}://{{host_name}}/data/{{series}}4
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "A title"
jsonpath "$['dcterms:description']" == "Description"
jsonpath "$['@id']" == "{{series}}4"
jsonpath "$['@type']" == "dh:DatasetSeries"
jsonpath "$['@context'].['dh']" == "https://publishmydata.com/def/datahost/"
jsonpath "$['@context'].['dcterms']" == "http://purl.org/dc/terms/"
jsonpath "$['@context'].['rdf']" == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
jsonpath "$['@context'].['rdfs']" == "http://www.w3.org/2000/01/rdf-schema#"
jsonpath "$['@context'].['dcat']" == "http://www.w3.org/ns/dcat#"
jsonpath "$['@context'].['csvw']" == "http://www.w3.org/ns/csvw#"
jsonpath "$['@context'].['appropriate-csvw']" == "https://publishmydata.com/def/appropriate-csvw/"
jsonpath "$['@context'].['@base']" == "{{expected_scheme}}://{{expected_uri_root}}/data/"


#Multiple releases can be can be retrieved via the API
PUT {{scheme}}://{{host_name}}/data/{{series}}4
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "A Second Series",
    "dcterms:description": "Description 2"
}
HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "A Second Series"
jsonpath "$['dcterms:description']" == "Description 2"
[Captures]
Series2Modified: jsonpath "$['dcterms:modified']"

GET {{scheme}}://{{host_name}}/data/{{series}}4
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "A Second Series"
jsonpath "$['dcterms:description']" == "Description 2"




#A release can be updated, query params take precedence

PUT {{scheme}}://{{host_name}}/data/{{series}}4
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
jsonpath "$['dcterms:modified']" != "{{Series2Modified}}"
[Captures]
Series2Modified2: jsonpath "$['dcterms:modified']"

#"No update when query params same as in existing doc"
PUT {{scheme}}://{{host_name}}/data/{{series}}4
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
[QueryStringParams]
title: A query params update

HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "A query params update"
jsonpath "$['dcterms:modified']" == "{{Series2Modified2}}"


#Deleting returns 204
DELETE {{scheme}}://{{host_name}}/data/{{series}}4
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 204

#Deleted series not there
GET {{scheme}}://{{host_name}}/data/{{series}}4
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

#Delete series with release
PUT {{scheme}}://{{host_name}}/data/{{series}}5
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201


PUT {{scheme}}://{{host_name}}/data/{{series}}5/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201


DELETE {{scheme}}://{{host_name}}/data/{{series}}5
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 204

GET {{scheme}}://{{host_name}}/data/{{series}}5
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}5/release/release-1.json
Authorization: {{auth_token}}

HTTP 404


#Delete series with release and schema
PUT {{scheme}}://{{host_name}}/data/{{series}}6
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201


PUT {{scheme}}://{{host_name}}/data/{{series}}6/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}6/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-qualifications-by-area.json;

HTTP 201


DELETE {{scheme}}://{{host_name}}/data/{{series}}6
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 204

GET {{scheme}}://{{host_name}}/data/{{series}}6
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}6/release/release-1.json
Authorization: {{auth_token}}

HTTP 404


GET {{scheme}}://{{host_name}}/data/{{series}}6/release/release-1/schema
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404



#Delete series with release and schema and revision
PUT {{scheme}}://{{host_name}}/data/{{series}}7
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201


PUT {{scheme}}://{{host_name}}/data/{{series}}7/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}7/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-qualifications-by-area.json;

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}7/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test revision",
    "dcterms:description": "A very simple revision"
}

HTTP 201


DELETE {{scheme}}://{{host_name}}/data/{{series}}7
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 204

GET {{scheme}}://{{host_name}}/data/{{series}}7
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}7/release/release-1.json
Authorization: {{auth_token}}

HTTP 404


GET {{scheme}}://{{host_name}}/data/{{series}}7/release/release-1/schema
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}7/release/release-1/revision/revision-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404


#Delete series with release, schema, revision and change
PUT {{scheme}}://{{host_name}}/data/{{series}}8
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201


PUT {{scheme}}://{{host_name}}/data/{{series}}8/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}8/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-qualifications-by-area.json;

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}8/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test revision",
    "dcterms:description": "A very simple revision"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}8/release/release-1/revision/1/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}8
file,common/2019.csv;

HTTP 201


DELETE {{scheme}}://{{host_name}}/data/{{series}}8
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 204

GET {{scheme}}://{{host_name}}/data/{{series}}8
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}8/release/release-1.json
Authorization: {{auth_token}}

HTTP 404


GET {{scheme}}://{{host_name}}/data/{{series}}8/release/release-1/schema
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}8/release/release-1/revision/revision-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}8/release/release-1/revision/1/commit/1
Accept: text/csv
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

#test that creates two series with their own releases, revisions and a
#shared schema. Both contain a change with the same contents - these
#changes should have the same address within the store. The test deletes
#one of the series (and therefore its associated change) and checks that
#the contents of the change in the other series can still be fetched

PUT {{scheme}}://{{host_name}}/data/{{series}}9
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201

PUT {{scheme}}://{{host_name}}/data/{{series}}10
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201


PUT {{scheme}}://{{host_name}}/data/{{series}}9/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

PUT {{scheme}}://{{host_name}}/data/{{series}}10/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}9/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-qualifications-by-area.json;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}10/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-qualifications-by-area.json;

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}9/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test revision",
    "dcterms:description": "A very simple revision"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}10/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test revision",
    "dcterms:description": "A very simple revision"
}

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}9/release/release-1/revision/1/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}8
file,common/2019.csv;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}10/release/release-1/revision/1/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}8
file,common/2019.csv;

HTTP 201

DELETE {{scheme}}://{{host_name}}/data/{{series}}9
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 204

GET {{scheme}}://{{host_name}}/data/{{series}}9
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}9/release/release-1.json
Authorization: {{auth_token}}

HTTP 404


GET {{scheme}}://{{host_name}}/data/{{series}}9/release/release-1/schema
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}9/release/release-1/revision/revision-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}9/release/release-1/revision/1/commit/1
Accept: text/csv
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 404

GET {{scheme}}://{{host_name}}/data/{{series}}10/release/release-1/revision/1/commit/1
Accept: text/csv
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200