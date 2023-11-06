# Description: Creates a new series

PUT {{scheme}}://{{host_name}}/data/{{series}}
Authorization: Basic {{auth_token}}
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "Blackbox Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201
