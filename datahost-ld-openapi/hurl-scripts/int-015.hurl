#release schema test

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

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-qualifications-by-area.json;

HTTP 201

[Asserts]
jsonpath "$['@context'].['dh']" == "https://publishmydata.com/def/datahost/"
jsonpath "$['@context'].['dcterms']" == "http://purl.org/dc/terms/"
jsonpath "$['@context'].['csvw']" == "http://www.w3.org/ns/csvw#"
jsonpath "$['@context'].['appropriate-csvw']" == "https://publishmydata.com/def/appropriate-csvw/"
jsonpath "$['@context'].['@base']" == "{{expected_scheme}}://{{expected_uri_root}}/data/"

jsonpath "$['@id']" == "{{series}}/release/release-1/schema"
jsonpath "$['dh:appliesToRelease']" == "{{expected_scheme}}://{{expected_uri_root}}/data/{{series}}/release/release-1"
jsonpath "$['appropriate-csvw:modeling-of-dialect']" == "UTF-8,RFC4180"
jsonpath "$['dh:columns'].[0].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[0].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[0].['csvw:name']" == "Measure type"
jsonpath "$['dh:columns'].[0].['csvw:number']" == "1"
jsonpath "$['dh:columns'].[0].['csvw:titles']" == "Measure type"

jsonpath "$['dh:columns'].[1].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[1].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[1].['csvw:name']" == "Statistical Geography"
jsonpath "$['dh:columns'].[1].['csvw:number']" == "2"
jsonpath "$['dh:columns'].[1].['csvw:titles']" == "Statistical Geography"

jsonpath "$['dh:columns'].[2].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[2].['csvw:datatype']" == "integer"
jsonpath "$['dh:columns'].[2].['csvw:name']" == "Year"
jsonpath "$['dh:columns'].[2].['csvw:number']" == "3"
jsonpath "$['dh:columns'].[2].['csvw:titles']" == "Year"

jsonpath "$['dh:columns'][3]['@type']" == "dh:MeasureColumn"
jsonpath "$['dh:columns'][3]['csvw:datatype']" == "double"
jsonpath "$['dh:columns'][3]['csvw:name']" == "Aged 16 to 64 years level 3 or above qualifications"
jsonpath "$['dh:columns'][3]['csvw:number']" == "4"
jsonpath "$['dh:columns'][3]['csvw:titles']" == "Aged 16 to 64 years level 3 or above qualifications"

jsonpath "$['dh:columns'].[4].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[4].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[4].['csvw:name']" == "Unit of Measure"
jsonpath "$['dh:columns'].[4].['csvw:number']" == "5"
jsonpath "$['dh:columns'].[4].['csvw:titles']" == "Unit of Measure"

jsonpath "$['dh:columns'].[5].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[5].['csvw:datatype']" == "double"
jsonpath "$['dh:columns'].[5].['csvw:name']" == "Upper Confidence Interval"
jsonpath "$['dh:columns'].[5].['csvw:number']" == "6"
jsonpath "$['dh:columns'].[5].['csvw:titles']" == "Upper Confidence Interval"

jsonpath "$['dh:columns'].[6].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[6].['csvw:datatype']" == "double"
jsonpath "$['dh:columns'].[6].['csvw:name']" == "Lower Confidence Interval"
jsonpath "$['dh:columns'].[6].['csvw:number']" == "7"
jsonpath "$['dh:columns'].[6].['csvw:titles']" == "Lower Confidence Interval"

jsonpath "$['dh:columns'].[7].['@type']" == "dh:AttributeColumn"
jsonpath "$['dh:columns'].[7].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[7].['csvw:name']" == "Observation Status"
jsonpath "$['dh:columns'].[7].['csvw:number']" == "8"
jsonpath "$['dh:columns'].[7].['csvw:titles']" == "Observation Status"
jsonpath "$['@type']" == "dh:TableSchema"
jsonpath "$['dcterms:title']" == "Dummy Schema"


#"The release was updated with a reference to the schema"

GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1.json
Authorization: {{auth_token}}

HTTP 200
[Asserts]

jsonpath "$['dh:hasSchema']" endsWith "{{series}}/release/release-1/schema"

#The schema can be retrieved
GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
#jsonpath "$['@context'].['dh']" == "https://publishmydata.com/def/datahost/"
#jsonpath "$['@context'].['dcterms']" == "http://purl.org/dc/terms/"
#jsonpath "$['@context'].['csvw']" == "http://www.w3.org/ns/csvw#"
#jsonpath "$['@context'].['appropriate-csvw']" == "https://publishmydata.com/def/appropriate-csvw/"
jsonpath "$['@context'].[1].['@base']" == "{{expected_scheme}}://{{expected_uri_root}}/data/{{series}}/release/release-1/"

jsonpath "$['@id']" == "schema"
jsonpath "$['dh:appliesToRelease']" == "{{expected_scheme}}://{{expected_uri_root}}/data/{{series}}/release/release-1"
jsonpath "$['appropriate-csvw:modeling-of-dialect']" == "UTF-8,RFC4180"
jsonpath "$['dh:columns'].[0].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[0].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[0].['csvw:name']" == "Measure type"
jsonpath "$['dh:columns'].[0].['csvw:number']" == 1
jsonpath "$['dh:columns'].[0].['csvw:titles']" == "Measure type"

jsonpath "$['dh:columns'].[1].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[1].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[1].['csvw:name']" == "Statistical Geography"
jsonpath "$['dh:columns'].[1].['csvw:number']" == 2
jsonpath "$['dh:columns'].[1].['csvw:titles']" == "Statistical Geography"

jsonpath "$['dh:columns'].[2].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[2].['csvw:datatype']" == "integer"
jsonpath "$['dh:columns'].[2].['csvw:name']" == "Year"
jsonpath "$['dh:columns'].[2].['csvw:number']" == 3
jsonpath "$['dh:columns'].[2].['csvw:titles']" == "Year"

jsonpath "$['dh:columns'][3]['@type']" == "dh:MeasureColumn"
jsonpath "$['dh:columns'][3]['csvw:datatype']" == "double"
jsonpath "$['dh:columns'][3]['csvw:name']" == "Aged 16 to 64 years level 3 or above qualifications"
jsonpath "$['dh:columns'][3]['csvw:number']" == 4
jsonpath "$['dh:columns'][3]['csvw:titles']" == "Aged 16 to 64 years level 3 or above qualifications"

jsonpath "$['dh:columns'].[4].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[4].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[4].['csvw:name']" == "Unit of Measure"
jsonpath "$['dh:columns'].[4].['csvw:number']" == 5
jsonpath "$['dh:columns'].[4].['csvw:titles']" == "Unit of Measure"

jsonpath "$['dh:columns'].[5].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[5].['csvw:datatype']" == "double"
jsonpath "$['dh:columns'].[5].['csvw:name']" == "Upper Confidence Interval"
jsonpath "$['dh:columns'].[5].['csvw:number']" == 6
jsonpath "$['dh:columns'].[5].['csvw:titles']" == "Upper Confidence Interval"

jsonpath "$['dh:columns'].[6].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[6].['csvw:datatype']" == "double"
jsonpath "$['dh:columns'].[6].['csvw:name']" == "Lower Confidence Interval"
jsonpath "$['dh:columns'].[6].['csvw:number']" == 7
jsonpath "$['dh:columns'].[6].['csvw:titles']" == "Lower Confidence Interval"

jsonpath "$['dh:columns'].[7].['@type']" == "dh:AttributeColumn"
jsonpath "$['dh:columns'].[7].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[7].['csvw:name']" == "Observation Status"
jsonpath "$['dh:columns'].[7].['csvw:number']" == 8
jsonpath "$['dh:columns'].[7].['csvw:titles']" == "Observation Status"
jsonpath "$['@type']" == "dh:TableSchema"
jsonpath "$['dcterms:title']" == "Dummy Schema"


#minimal column count schema test
PUT {{scheme}}://{{host_name}}/data/{{series}}2
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201


PUT {{scheme}}://{{host_name}}/data/{{series}}2/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}2/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
{
"dcterms:title": "Fun schema",
"dcterms:description": "Description",
"dh:columns":[{"@type": "dh:MeasureColumn",
             "csvw:datatype": "string",
             "csvw:name": "measure",
             "csvw:titles": "Measure"},
             {"@type": "dh:DimensionColumn",
             "csvw:datatype": "string",
             "csvw:name": "dim",
             "csvw:titles": "Dimension"}]
}
HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}2/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 200
[Asserts]
jsonpath "$['dcterms:title']" == "Fun schema"
jsonpath "$['dcterms:description']" == "Description"
jsonpath "$['dh:columns'].[0].['@type']" == "dh:MeasureColumn"
jsonpath "$['dh:columns'].[0].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[0].['csvw:name']" == "measure"
jsonpath "$['dh:columns'].[0].['csvw:titles']" == "Measure"
jsonpath "$['dh:columns'].[1].['@type']" == "dh:DimensionColumn"
jsonpath "$['dh:columns'].[1].['csvw:datatype']" == "string"
jsonpath "$['dh:columns'].[1].['csvw:name']" == "dim"
jsonpath "$['dh:columns'].[1].['csvw:titles']" == "Dimension"