const csv = require('csv-parser')
const fs = require('fs')
const crypto = require('crypto'),
    hash = crypto.getHashes();
const results = [];
const outputFile = 'test.csv'
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const args = process.argv.slice(2);
const original = args[0];
const current = args[1];

console.log(original)

console.time("Total time");
const observations = "Aged 16 to 64 years level 3 or above qualifications"

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
    let temp = []
    for (i = 0; i < data.length; i++) {
        let toHash = JSON.stringify(data[i])
        let rowHash = crypto.createHash('sha1').update(toHash).digest('hex');
        //we only need the hash for the old data so just produce an array of those
        if (type === "old") {
            temp[i] = data[i]["fullHash"] = rowHash;
        } else {
            temp = data
            temp[i]["fullHash"] = rowHash;
        }
    }
    return temp
}

compareData = async (old, current) => {
    data = []
    for (i = 0; i < current.length; i++) {
        let hash = current[i].fullHash
        let result = old.find(fullHash => fullHash === hash);
        if (result == undefined) {
            data.push(current[i])
        }
    }
    return data
}

generateHeaders = async (row) => {
    let headers = []
    //using row.length-1 to not include hash in output
    for (i = 0; i < row.length - 1; i++) {
        let item = {}
        item["id"] = row[i];
        item["title"] = row[i];
        headers.push(item)
    }
    return headers
}

start = async () => {

    console.log("Loading data")
    console.time("Loading data");
    const existingData = await getOldData(original)
    const currentData = await getCurrentData(current)
    console.timeEnd("Loading data");

    console.log("Generating hashed values")
    console.time("Generating hashed values");
    let existingDataHash = await fullRowHash(existingData, "old")
    let currentDataHash = await fullRowHash(currentData)
    console.timeEnd("Generating hashed values");

    console.log("Compare data")
    console.time("Compare data");
    let noExact = await compareData(existingDataHash, currentDataHash)
    console.timeEnd("Compare data");

    console.log("Generate download")
    console.time("Generate download");
    let headers = await generateHeaders(Object.keys(existingData[0]))
    const csvWriter = createCsvWriter({
        path: outputFile,
        header: headers
    });

    csvWriter.writeRecords(noExact)
        .then(() => {
            console.log('Complete');
        }).catch(function (error) {
            console.log(error);
        });
    console.timeEnd("Generate download");
    console.timeEnd("Total time");
}

start()