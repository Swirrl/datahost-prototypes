## Description: API interaction with multiple commits per revision

PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"

PUT {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/schema
Content-Type: application/json
file,common/schema-name-age.json;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "Rev 1",
    "dcterms:description": "A test revision"
}

HTTP 201
[Captures]
revision1_url: header "Location"


POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
[QueryStringParams]
title: changes appends
description: change for {{series}}
format: text/csv
file,issue-253/1.csv;

HTTP 201


POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
[QueryStringParams]
title: changes appends
description: change for {{series}}
format: text/csv
file,issue-253/2.csv;

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "Rev 2",
    "dcterms:description": "A test revision"
}

HTTP 201
[Captures]
revision2_url: header "Location"


POST {{scheme}}://{{host_name}}{{revision2_url}}/corrections
Content-Type: text/csv
[QueryStringParams]
title: changes corrections
description: change for {{series}}
format: text/csv
file,issue-253/3.csv;

HTTP 201


POST {{scheme}}://{{host_name}}{{revision2_url}}/retractions
Content-Type: text/csv
[QueryStringParams]
title: changes retractions
description: change for {{series}}
format: text/csv
file,issue-253/4.csv;

HTTP 201

# let's try to append to revision 1 **after** revision 2 was created.

POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
[QueryStringParams]
title: changes appends
description: change for {{series}}
format: text/csv
file,issue-253/1.csv;

HTTP 422

GET {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json

HTTP 200

GET {{scheme}}://{{host_name}}{{revision2_url}}
Accept: text/csv

HTTP 200
```
name,age
cindy,29
fabio,25
max,44
```

# run: hurl FILE_NAME --variable series=series-01
