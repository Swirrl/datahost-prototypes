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

fullRowHash = async (data, type) => {
    let temp = structuredClone(data)

    //we only need the hash for the old data so just produce an array of those
    if (type === "old") {
        for (i = 0; i < data.length; i++) {
            let toHash = JSON.stringify(data[i])
            toHash = toHash.toLowerCase()
            let rowHash = crypto.createHash('sha1').update(toHash).digest('hex');
            temp[i] = rowHash;
        }
    } else {
        for (i = 0; i < data.length; i++) {
            let toHash = JSON.stringify(data[i])
            toHash = toHash.toLowerCase()
            let rowHash = crypto.createHash('sha1').update(toHash).digest('hex');
            temp[i]["fullHash"] = rowHash;
        }
    }

    return temp
}

compareData = async (old, current) => {
    data = []
    for (i = 0; i < current.length; i++) {
        if (i % 10000 === 0) {
            console.log(`Compared: ${i}`)
        }
        let hash = current[i].fullHash
        result = old.indexOf(hash)
        if (result === -1) {
            data.push(current[i])
        }
    }
    return data
}

compareDataCorrections = async (old, current) => {
    corrections = []
    appends = []
    for (i = 0; i < current.length; i++) {
        let hash = current[i].fullHash
        result = old.indexOf(hash)
        if (result != -1) {
            //dimensions match so must be a correction
            corrections.push(i)
        } else {
            //dimensions don't match so assume it is new
            appends.push(i)
        }
    }
    return [corrections, appends]
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

stripOutHash = async (data) => {
    for (i = 0; i < data.length; i++) {
        delete data[i]["fullHash"];
    }
    return data
}


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

checkHeaders = (original, current) => {
    let originalHeaders = Object.keys(original[0])
    let currentHeaders = Object.keys(current[0])
    if (JSON.stringify(originalHeaders) === JSON.stringify(currentHeaders)) {
        return true
    } else {
        console.log("Headers don't match, please check and rerun")
        console.log(JSON.stringify(originalHeaders))
        console.log(JSON.stringify(currentHeaders))
    }
}

start = async () => {

    console.log("Loading data")
    console.time("Loading data");
    const existingData = await getOldData(original)
    const currentData = await getCurrentData(current)
    console.timeEnd("Loading data");

    console.log("Checking headers")
    const headersMatch = await checkHeaders(existingData, currentData)

    if (headersMatch === true) {
        console.log("Headers match")

        console.log("Generating hashed values")
        console.time("Generating hashed values");
        let existingDataHash = await fullRowHash(existingData, "old")
        let currentDataHash = await fullRowHash(currentData)
        console.log(currentDataHash)
        console.timeEnd("Generating hashed values");


        console.log("Compare data")
        console.time("Compare data");
        let noExact = await compareData(existingDataHash, currentDataHash)
        console.timeEnd("Compare data");

        let noExactNoHash = await stripOutHash(noExact)

        //if there are no changes don't do anything else
        if (noExactNoHash != 0) {

            const existingDataDimensions = await removeObservationsForHashing(existingData, observations)
            const currentDataDimensions = await removeObservationsForHashing(noExactNoHash, observations)

            console.log("Generating hashed values")
            console.time("Generating hashed values");
            let existingDataHashDimensions = await fullRowHash(existingDataDimensions, "old")
            let currentDataHashDimensions = await fullRowHash(currentDataDimensions)
            console.timeEnd("Generating hashed values");

            console.log("Compare data")
            console.time("Compare data");
            let [correctionsIndex, appendsIndex] = await compareDataCorrections(existingDataHashDimensions, currentDataHashDimensions)
            console.timeEnd("Compare data");

            console.log("Generate downloads")
            console.time("Generate downloads");
            let corrections = []
            for (i = 0; i < correctionsIndex.length; i++) {
                temp = correctionsIndex[i]
                corrections.push(noExactNoHash[temp])
            }

            let appends = []
            for (i = 0; i < appendsIndex.length; i++) {
                temp = appendsIndex[i]
                appends.push(noExactNoHash[temp])
            }


            if (appends.length != 0) {
                let appendsHeader = await generateHeaders(Object.keys(appends[0]))
                const csvWriter = createCsvWriter({
                    path: outputPrefix + "-appends.csv",
                    header: appendsHeader
                });

                if (test === "test") {
                    console.log("Appends:")
                    console.log(appends)
                    console.log("--------")
                }


                csvWriter.writeRecords(appends)
                    .then(() => {
                        console.log('Complete appends file created');
                    }).catch(function (error) {
                        console.log(error);
                    });
            }

            if (corrections.length != 0) {
                let correctionsHeader = await generateHeaders(Object.keys(corrections[0]))
                const csvWriter2 = createCsvWriter({
                    path: outputPrefix + "-corrections.csv",
                    header: correctionsHeader
                });

                if (test === "test") {
                    console.log("Corrections:")
                    console.log(corrections)
                    console.log("--------")
                }

                csvWriter2.writeRecords(corrections)
                    .then(() => {
                        console.log('Complete corrections file created');
                    }).catch(function (error) {
                        console.log(error);
                    });

            }
            console.timeEnd("Generate downloads");
            console.timeEnd("Total time");
        } else {
            console.log("No changes between datasets")
        }
    }
}

start()