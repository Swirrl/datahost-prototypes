POST http://localhost:3000{{revision_url}}/appends
Accept: application/json
Content-Type: text/csv
[QueryStringParams]
description: Add data for liverpool
format: text/csv
```
area,age_range,estimate
liverpool,16-20,103.0
manchester,16-20,100.0
leeds,16-20,99.0
```

HTTP 201
