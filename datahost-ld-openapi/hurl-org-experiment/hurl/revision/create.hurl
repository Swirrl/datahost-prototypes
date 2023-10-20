POST http://localhost:3000/data/{{series}}/releases/{{release}}/revisions
Accept: application/json
Content-Type: application/json
{
 "dcterms:title": "Test revision",
 "dcterms:description": "Test revision"
}

HTTP 201
[Captures]
revision_url: header "Location"
