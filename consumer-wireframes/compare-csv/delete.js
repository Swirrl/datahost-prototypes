const csv = require('csv-parser')
const fs = require('fs')
const crypto = require('crypto'),
    hash = crypto.getHashes();
const results = [];
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const args = process.argv.slice(2);
const original = args[0];
const current = args[1];
let outputPrefix = args[2];
const observations = args[3];
const test = args[4];

if (test === "test") {
    outputPrefix = "test-data/outputs/" + outputPrefix
} else {
    outputPrefix = "outputs/" + outputPrefix
}

console.time("Total time");

//get files
getOldData = async (url) => {
    return new Promise(function (resolve, reject) {
        fs.createReadStream(url)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => {
                resolve(results)
            })
            .on('error', reject);
    })
}
getCurrentData = async (url) => {
    let results2 = []
    return new Promise(function (resolve, reject) {
        fs.createReadStream(url)
            .pipe(csv())
            .on('data', (data2) => results2.push(data2))
            .on('end', () => {
                resolve(results2)
            })
            .on('error', reject);
    })
}

//remove observations
removeObservationsForHashing = (data, observationColumn) => {
    h = Object.keys(data[0])
    let removedObservations = []
    for (i = 0; i < data.length; i++) {
        let item = {}
        for (j = 0; j < h.length; j++) {
            let header = h[j]
            if (header != observationColumn) {
                item[header] = data[i][header];
            }
        }
        removedObservations.push(item)
    }
    return removedObservations
}

//hash rows to array
fullRowHash = async (data) => {
    let temp = structuredClone(data)

    for (i = 0; i < data.length; i++) {
        let toHash = JSON.stringify(data[i])
        toHash = toHash.toLowerCase()
        let rowHash = crypto.createHash('sha1').update(toHash).digest('hex');
        temp[i] = rowHash;
    }
    return temp
}

compareData = async (old, current) => {
    deletes = []
    for (i = 0; i < old.length; i++) {
        if (i % 25000 === 0 && i > 1) {
            console.log(`Compared: ${i}`)
        }
        let hash = old[i]
        result = current.indexOf(hash)
        if (result == -1) {
            deletes.push(i)
        }
    }
    return deletes
}

generateHeaders = async (row) => {
    let headersOutput = []
    for (i = 0; i < row.length; i++) {
        let item = {}
        item["id"] = row[i];
        item["title"] = row[i];
        headersOutput.push(item)
    }
    return headersOutput
}


start = async () => {
    //get files
    console.log("Loading data")
    console.time("Loading data");
    const existingData = await getOldData(original)
    const currentData = await getCurrentData(current)
    console.timeEnd("Loading data");

    //remove observations
    console.log("Removing observations")
    console.time("Removing observations");
    const existingDataDimensions = await removeObservationsForHashing(existingData, observations)
    const currentDataDimensions = await removeObservationsForHashing(currentData, observations)
    console.timeEnd("Removing observations");

    //hash rows to array
    console.log("Generating hashed values")
    console.time("Generating hashed values");
    let existingDataHash = await fullRowHash(existingDataDimensions)
    let currentDataHash = await fullRowHash(currentDataDimensions)
    console.timeEnd("Generating hashed values");

    //compare array and get index of deletes
    console.log("Compare data")
    console.time("Compare data");
    let deletesIndex = await compareData(existingDataHash, currentDataHash)
    console.timeEnd("Compare data");

    //find indexed objects in original file
    console.log("Lookup deleted values")
    console.time("Lookup deleted values");
    let deletes = []
    for (i = 0; i < deletesIndex.length; i++) {
        temp = deletesIndex[i]
        deletes.push(existingData[temp])
    }
    console.timeEnd("Lookup deleted values");

    if (deletes.length != 0) {
        if (test === "test") {
            console.log("Deletes:")
            console.log(deletes)
            console.log("--------")
        }

        //write file to csv
        let deletesHeader = await generateHeaders(Object.keys(deletes[0]))
        const csvWriter2 = createCsvWriter({
            path: outputPrefix + "-deletes.csv",
            header: deletesHeader
        });

        csvWriter2.writeRecords(deletes)
            .then(() => {
                console.log('Complete deletes file created');
            }).catch(function (error) {
                console.log(error);
            });
            console.timeEnd("Total time");
    } else {
        console.timeEnd("Total time");
        console.log("No deleted values found")
    }
    
}

start()