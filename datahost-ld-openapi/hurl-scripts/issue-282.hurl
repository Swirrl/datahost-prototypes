# Description: We try to append a data with incorrectly formatted 'double' value,
# and that should be rejected by the API.

PUT {{scheme}}://{{host_name}}/data/{{series}}
Content-Type: application/json
{"dcterms:description": "Crimes Data - Float coercion test",
 "dcterms:title": "Column float coercion test"}

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}

HTTP 200

PUT {{scheme}}://{{host_name}}/data/{{series}}/release/{{release}}
Content-Type: application/json
{"dcterms:description": "Release: {{release}}",
"dcterms:title": "{{release}}"}

GET {{scheme}}://{{host_name}}/data/{{series}}/release/{{release}}.json

HTTP 200

POST {{scheme}}://{{host_name}}/data/{{series}}/release/{{release}}/schema
Content-Type: application/json
{
 "@type": "dh:TableSchema",
 "dcterms:title": "{{release}} schema",
 "dh:columns": [
		{
		 "@type": "dh:DimensionColumn",
		 "csvw:datatype": "string",
		 "csvw:name": "area",
                 "csvw:titles": "area"
                 },
                {
		 "@type": "dh:DimensionColumn",
		 "csvw:datatype": "string",
		 "csvw:name": "age_range",
                 "csvw:titles": "age_range"
                 },
                 {
		 "@type": "dh:MeasureColumn",
		 "csvw:datatype": "double",
		 "csvw:name": "estimate",
                 "csvw:titles": "Estimate"
                 }
	        ]
 }

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/{{release}}/revisions
Accept: application/json
Content-Type: application/json
{
 "dcterms:title": "Test revision",
 "dcterms:description": "Test revision"
}

HTTP 201
[Captures]
revision_url: header "Location"

GET {{scheme}}://{{host_name}}{{revision_url}}
Accept: application/json

HTTP 200

## One append

POST {{scheme}}://{{host_name}}{{revision_url}}/appends
Accept: application/json
Content-Type: text/csv
[QueryStringParams]
description: Add data for liverpool
format: text/csv
```
area,age_range,estimate
liverpool,16-20,103.5
manchester,16-20,100
leeds,16-20,99.2
```

HTTP 400
