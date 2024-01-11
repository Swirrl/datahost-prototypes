#Check page is up.

GET http://localhost:8887/
Accept: application/json
Content-Type: application/json
HTTP 200

#Check that it is not an error page.

GET http://localhost:8887/datasets/
HTTP 200
[Asserts]
xpath "string(/html/body/main/div[3]/div/div/p)" contains "Results"


#Check that the correct data is being displayed TODO

