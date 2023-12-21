populateTable = data => {
    if(data.length < tableLength) {
        rows = data.length
    } else {
        rows = tableLength
    }
    for (i = 0; i < rows; i++) {
        let newRow = tableRef.insertRow(-1);

        for (j = 0; j < tableConfig.length; j++) {
            let Cell = newRow.insertCell(j);
            let Text = document.createTextNode(data[i][tableConfig[j].name]);
            Cell.appendChild(Text);
        }
    }
    document.getElementById("loader").hidden = true
    document.getElementById("datasetList").hidden = false
};

populateDatasetSeries = data => {
    document.getElementById("datasetID").innerHTML = `<a href="http://localhost:3000/data/${data["@id"]}" target="_blank">${data["@id"]}</a>`
    document.getElementById("datasetTitle").innerHTML = data["dcterms:title"]
    document.getElementById("datasetDescription").innerHTML = data["dcterms:description"]
    document.getElementById("loader").hidden = true
    document.getElementById("datasetSeries").hidden = false
}