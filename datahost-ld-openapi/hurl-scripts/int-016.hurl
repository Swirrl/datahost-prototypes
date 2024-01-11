#This is for checking the front end wireframe is up and running - more tests are needed to
#ensure it displays the correct data from the backend

#Check page is up.

GET {{scheme}}://localhost:8080/
Accept: application/json
Content-Type: application/json
HTTP 200

#Check that it is not an error page.

GET {{scheme}}://localhost:8080/datasets/
HTTP 200
[Asserts]
xpath "string(/html/body/main/div[3]/div/div/p)" contains "Results"


#Check that the correct data is being displayed TODO

