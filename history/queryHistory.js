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
const SampleData = require('../seed/sampleData');
const { prettyPrintResultList } = require('../query/scanTable');
const util = require('../seed/util');

/**
 * Find previous primary owners for the given VIN in a single transaction.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param vin The VIN to find previous primary owners for.
 * @returns Promise which fulfills with void.
 */
async function previousPrimaryOwners(txn, vin) {
    const documentId = await util.getDocumentId(txn, 'VehicleRegistration', "VIN", vin);
    const todaysDate = new Date();
    const threeMonthsAgo = new Date(todaysDate);
    threeMonthsAgo.setMonth(todaysDate.getMonth() - 3);

    const query =
        `SELECT data.Owners.PrimaryOwner, metadata.version FROM history ` +
        `(VehicleRegistration) ` +
        `AS h WHERE h.metadata.id = ?`;
    console.log(query);
    const qldbWriter = createQldbWriter();
    util.writeValueAsIon(documentId, qldbWriter);

    await txn.executeInline(query, [qldbWriter]).then((result) => {
        console.log(`Querying the 'VehicleRegistration' table's history using VIN: ${vin}.`);
        const resultList = result.getResultList();
        prettyPrintResultList(resultList);
    });
}

/**
 * Query a table's history for a particular set of documents.
 * @returns Promise which fulfills with void.
 */
var main = async function() {
    let session;
    try {
        session = await createQldbSession();
        const vin = SampleData.VEHICLE_REGISTRATION[0].VIN;
        await session.executeLambda(async (txn) => {
            await previousPrimaryOwners(txn, vin);
        }, () => console.log("Retrying due to OCC conflict..."));
    } catch (e) {
        console.error(`Unable to query history to find previous owners: ${e}`);
    } finally {
        closeQldbSession(session);
    }
}

if (require.main === module) {
    main();
}