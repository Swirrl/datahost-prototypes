# Description: We try to append a data with incorrectly formatted 'double' value,
# and that should be rejected by the API.

# append with invalid double value

POST {{scheme}}://{{host_name}}/data/{{series}}/release/{{release}}/revision/1/appends
Accept: application/json
Content-Type: text/csv
[QueryStringParams]
description: Add data for liverpool
format: text/csv
```
area,age_range,estimate
manchester,16-20,invalid_double
```

HTTP 422
