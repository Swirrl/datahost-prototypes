PUT http://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "Series",
    "dcterms:description": "Series for delta test"
}

HTTP 201

PUT http://{{host_name}}/data/{{series}}/release/rel-1
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "Release 1",
    "dcterms:description": "First Release."
}

HTTP 201

POST http://{{host_name}}/data/{{series}}/release/rel-1/schema
Content-Type: application/json
{
	"@type": "dh:TableSchema",
	"appropriate-csvw:modeling-of-dialect": "UTF-8,RFC4180",
	"dh:appliesToRelease": "http://{{host_name}}/data/test-032/releases/release-1",
	"dcterms:title": "Fun schema",
	"dh:columns": [
		{
			"@type": "dh:DimensionColumn",
			"csvw:datatype": "string",
			"csvw:name": "name",
            "csvw:titles": "name"
        },
        {
			"@type": "dh:MeasureColumn",
			"csvw:datatype": "int",
			"csvw:name": "age",
            "csvw:titles": "age"
        }
	]
}

HTTP 201

POST http://{{host_name}}/data/{{series}}/release/rel-1/revisions
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "rev 1",
    "dcterms:description": "first revision."
}

HTTP 201
[Captures]
revision1_url: header "Location"

POST http://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
[QueryStringParams]
title: A schema
description: appends {{series}}/revision1_url
format: text/csv
```
name,age
bob,25
cindy,29
max,33
```

HTTP 201

# Requesting delta for 1

POST http://{{host_name}}/data/{{series}}/release/rel-1
Content-Type: text/csv
Accept: text/csv
#Accept: application/x-datahost-tx-csv
#If-Match: <REVISION_ID>
```
name,age,comment
bob,25,   ::no change
max,44,   ::correction
will,50,  ::append
```

HTTP 200

#[Capture]
#etag: header "Etag"
#```
#name,age,dh/kind,dh/id,dh/tx,dh/parent
#max,33,DELETE,<ID>,<TX>,
#max,44,APPEND,<ID>,<TX>,<ID>
#cindy,33,DELETE,<ID>,<TX>,
#will,50,APPEND,<ID>,<TX>,
#```
