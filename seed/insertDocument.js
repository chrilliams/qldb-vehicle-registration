/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
const { createQldbWriter, QldbSession, QldbWriter, Result, TransactionExecutor } = require('amazon-qldb-driver-nodejs');

const { Reader } = require('ion-js');

const { closeQldbSession, createQldbSession } = require('../custom_resources/QLDBHelpers/ConnectToLedger');
const SampleData = require('./sampleData');

var util = require('./util.js');


/**
 * Insert the given list of documents into a table in a single transaction.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param tableName Name of the table to insert documents into.
 * @param documents List of documents to insert.
 * @returns Promise which fulfills with a {@linkcode Result} object.
 */
async function insertDocument(txn,
    tableName,
    documents
){
    const statement = `INSERT INTO ${tableName} ?`;
    const documentsWriter = createQldbWriter();
    util.writeValueAsIon(documents, documentsWriter);
    let result = await txn.executeInline(statement, [documentsWriter]);
    return result;
}

/**
 * Handle the insertion of documents and updating PersonIds all in a single transaction.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @returns Promise which fulfills with void.
 */
async function updateAndInsertDocuments(txn ) {
    console.log("Inserting multiple documents into the 'Person' table...");
    const documentIds = await insertDocument(txn, 'Person', SampleData.PERSON);

    const listOfDocumentIds = documentIds.getResultList();
    console.log("Updating PersonIds for 'DriversLicense' and PrimaryOwner for 'VehicleRegistration'...");
    updatePersonId(listOfDocumentIds);

    console.log("Inserting multiple documents into the remaining tables...");
    await Promise.all([
        insertDocument(txn, 'DriversLicense', SampleData.DRIVERS_LICENSE),
        insertDocument(txn, 'VehicleRegistration', SampleData.VEHICLE_REGISTRATION),
        insertDocument(txn, 'Vehicle', SampleData.VEHICLE)
    ]);
}

/**
 * Update the PersonId value for DriversLicense records and the PrimaryOwner value for VehicleRegistration records.
 * @param documentIds List of document IDs.
 */
function updatePersonId(documentIds) {
    documentIds.forEach((reader, i) => {
        const documentId= util.getFieldValue(reader, ["documentId"]);
        SampleData.DRIVERS_LICENSE[i].PersonId = documentId;
        SampleData.VEHICLE_REGISTRATION[i].Owners.PrimaryOwner.PersonId = documentId;
    });
}

/**
 * Insert documents into a table in a QLDB ledger.
 * @returns Promise which fulfills with void.
 */
var main = async function() {
    let session;
    try {
        session = await createQldbSession();
        await session.executeLambda(async (txn) => {
            await updateAndInsertDocuments(txn);
        }, () => console.log("Retrying due to OCC conflict..."));
    
    } finally {
        closeQldbSession(session);
    }
}

if (require.main === module) {
    main();
}