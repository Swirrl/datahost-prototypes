
// const graphQLRoot = "http://localhost:8888/";
const graphQLRoot = "https://graphql-prototype.gss-data.org.uk/api";
// const openAPI = "http://localhost:3000";
const openAPI = "https://ldapi-prototype.gss-data.org.uk/"

  fetchDataPost = async (query) => {
    const settings = {
        method: 'POST',
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        },
        body: query
    };
    try {
        const fetchResponse = await fetch(graphQLRoot, settings);
        const data = await fetchResponse.json();
        return data;
    } catch (e) {
        return e;
    }    
}

fetchDataPut = async (query) => {
  const settings = {
      method: 'PUT',
      headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
      }
  };
  try {
      const fetchResponse = await fetch(query, settings);
      const data = await fetchResponse.json();
      return data;
  } catch (e) {
      return e;
  }    
}


getDatasetList = async () => {
    const datasetListQuery = document.getElementById("datasetListQuery");
    const datasetListResponse = document.getElementById("datasetListResponse");
    const datasetList = document.getElementById("datasetList");
    let query = `{
        endpoint {
          catalog {
            id
            catalog_query {
              datasets {
                id
                title
                comment
              }
            }
          }
        }
      }`;

    //structure the graphQL input to POST to API
    query = `{"query":${JSON.stringify(query)},"variables":null}}`
    
    const response = await fetchDataPost(query)
    
    datasetListQuery.innerHTML = query
    datasetListResponse.innerHTML = JSON.stringify(response)

    populateTable (response.data.endpoint.catalog.catalog_query.datasets)
}

searchForDataset = async (input) => {
  const datasetListQuery = document.getElementById("datasetListQuery");
  const datasetListResponse = document.getElementById("datasetListResponse");
  const datasetList = document.getElementById("datasetList");
  let query = `
  query textQuery($query_string: String) {
      endpoint {
        catalog {
          id
          catalog_query (search_string: $query_string) {
            datasets {
              id
              title
              comment
            }
          }
        }
      }
    }`;

  //structure the graphQL input to POST to API
  query = `{"query":${JSON.stringify(query)},"variables":{"query_string":"${input}"}}}`
  
  const response = await fetchDataPost(query)
  
  datasetListQuery.innerHTML = query
  datasetListResponse.innerHTML = JSON.stringify(response)

  populateTable (response.data.endpoint.catalog.catalog_query.datasets)
}


createDatasetSeries = async (query) => {

  document.getElementById("loader").hidden = false
  document.getElementById("datasetSeries").hidden = true
  const datasetSeriesQuery = document.getElementById("datasetSeriesQuery");
  const datasetSeriesResponse = document.getElementById("datasetSeriesResponse");
  const datasetSeries = document.getElementById("datasetSeries");

  const response = await fetchDataPut(query)
  
  datasetSeriesQuery.innerHTML = query
  datasetSeriesResponse.innerHTML = JSON.stringify(response)

  populateDatasetSeries (response)
}