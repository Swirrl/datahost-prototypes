# Description: We create and upload a dataset, then try to obtain a diff in the datahost-tx format, by uploading
# a modified version of the dataset.

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

# Requesting delta for release 1

POST http://{{host_name}}/data/{{series}}/release/rel-1
Content-Type: text/csv
Accept: text/csv
#Accept: application/x-datahost-tx-csv
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
#name,age,dh/op,datahost.row/id,datahost.row.id/previous
#max,33,retract,<ID>,<ID>,
#max,44,append,<ID>,<ID>,<ID>
#cindy,33,retract,<ID>,<ID>,
#will,50,append,<ID>,<ID>,
#```


# now let's verify the REVISION endpoint

POST http://{{host_name}}{{revision1_url}}
Content-Type: text/csv
Accept: text/csv
#Accept: application/x-datahost-tx-csv
```
name,age,comment
bob,25,   ::no change
max,44,   ::correction
will,50,  ::append
```

HTTP 200
#[Asserts]
#header "Etag" == "{{scheme}}://{{host_name}}{{revision1_url}}"

