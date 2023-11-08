#basic append delete and correction test

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

PUT {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Release",
    "dcterms:description": "A very simple Release"
}

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,data/well-being/release_1-1/release_1-1.json;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
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
file,data/well-being/release_1-1/wellbeing-local-authority-time-series-v1.csv;

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
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
file,data/well-being/release_1-1/wellbeing-local-authority-2-appends.csv;

HTTP 201


POST {{scheme}}://{{host_name}}{{revision2_url}}/deletes
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes deletes
description: change for {{series}}
file,data/well-being/release_1-1/wellbeing-local-authority-2-deletes.csv;

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 3",
    "dcterms:description": "A test revision"
}

HTTP 201
[Captures]
revision3_url: header "Location"


POST {{scheme}}://{{host_name}}{{revision3_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,data/well-being/release_1-1/wellbeing-local-authority-3-appends.csv;

HTTP 201


POST {{scheme}}://{{host_name}}{{revision3_url}}/deletes
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes deletes
description: change for {{series}}
file,data/well-being/release_1-1/wellbeing-local-authority-3-deletes.csv;

HTTP 201


POST {{scheme}}://{{host_name}}{{revision3_url}}/corrections
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes corrections
description: change for {{series}}
file,data/well-being/release_1-1/wellbeing-local-authority-3-corrections.csv;

HTTP 201

# run locally: hurl FILE_NAME --variable series=series-01 --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" 
# This Hurl script was created using hurl-gen.py with the following arguments- data/well-being/release_1-1/release_1-1.json revision appends:data/well-being/release_1-1/wellbeing-local-authority-time-series-v1.csv revision appends:data/well-being/release_1-1/wellbeing-local-authority-2-appends.csv deletes:data/well-being/release_1-1/wellbeing-local-authority-2-deletes.csv revision appends:data/well-being/release_1-1/wellbeing-local-authority-3-appends.csv deletes:data/well-being/release_1-1/wellbeing-local-authority-3-deletes.csv corrections:data/well-being/release_1-1/wellbeing-local-authority-3-corrections.csv
