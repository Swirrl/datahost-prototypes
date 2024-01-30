# Description: Ensure we accept 'double' values formatted without the decimal point.


POST {{scheme}}://{{host_name}}/data/{{series}}/release/{{release}}/revision/1/appends
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

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}/release/{{release}}/revision/1
Accept: text/csv

# Let's ensure the double '100' does not get turned into an integer

HTTP 200
[Asserts]
body matches /manchester,16-20,100.0/
