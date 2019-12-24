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

const { createQldbWriter, QldbSession, QldbWriter, Result, TransactionExecutor }  = require('amazon-qldb-driver-nodejs');

const { Reader } = require('ion-js');

const { closeQldbSession, createQldbSession } = require('../custom_resources/QLDBHelpers/ConnectToLedger');
const SampleData = require('../seed/sampleData');

const util =require('../seed/util');
const { prettyPrintResultList } = require('./scanTable');

/**
 * Query 'Vehicle' and 'VehicleRegistration' tables using a unique document ID in one transaction.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param govId The owner's government ID.
 * @returns Promise which fulfills with void.
 */
async function findVehiclesForOwner(txn, govId){
    const documentId = await util.getDocumentId(txn, 'Person', 'GovId', govId);
    const query = "SELECT Vehicle FROM Vehicle INNER JOIN VehicleRegistration AS r " +
                        "ON Vehicle.VIN = r.VIN WHERE r.Owners.PrimaryOwner.PersonId = ?";
    const qldbWriter = createQldbWriter();
    util.writeValueAsIon(documentId, qldbWriter);

    await txn.executeInline(query, [qldbWriter]).then((result) => {
        const resultList = result.getResultList();
        console.log(`List of vehicles for owner with GovId: ${govId}`);
        prettyPrintResultList(resultList);
    });
}

/**
 * Find all vehicles registered under a person.
 * @returns Promise which fulfills with void.
 */
var main = async function() {
    let session;
    try {
        session = await createQldbSession();
        await session.executeLambda(async (txn) => {
            await findVehiclesForOwner(txn, SampleData.PERSON[0].GovId);
        }, () => console.log("Retrying due to OCC conflict..."));
    } catch (e) {
        console.error(`Error getting vehicles for owner: ${e}`);
    } finally {
        closeQldbSession(session);
    }
}

if (require.main === module) {
    main();
}