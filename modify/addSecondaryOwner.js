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

const { makeReader, Reader } = require('ion-js');

const { closeQldbSession, createQldbSession } = require('../custom_resources/QLDBHelpers/ConnectToLedger');
const SampleData = require('../seed/sampleData');
const util = require('../seed/util');
const { prettyPrintResultList } = require('../query/scanTable');

/**
 * Add a secondary owner into 'VehicleRegistration' table for a particular VIN.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param vin VIN of the vehicle to query.
 * @param secondaryOwnerId The secondary owner's person ID.
 * @returns Promise which fulfills with void.
 */
async function addSecondaryOwner(
    txn, 
    vin, 
    secondaryOwnerId
) {
    console.log(`Inserting secondary owner for vehicle with VIN: ${vin}`);
    const query =
        `FROM VehicleRegistration AS v WHERE v.VIN = '${vin}' INSERT INTO v.Owners.SecondaryOwners VALUE ?`;

    const qldbWriter = createQldbWriter();
    const personToInsert = { PersonId: secondaryOwnerId };
    util.writeValueAsIon(personToInsert, qldbWriter);

    await txn.executeInline(query, [qldbWriter]).then(async (result) => {
        const resultList = result.getResultList();
        console.log("VehicleRegistration Document IDs which had secondary owners added: ");
        prettyPrintResultList(resultList);
    });
}

/**
 * Query for a document ID with a government ID.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param governmentId The government ID to query with.
 * @returns Promise which fulfills with the document ID as a string.
 */
async function getDocumentIdByGovId(txn, governmentId) {
    const documentId = await util.getDocumentId(txn, 'Person', "GovId", governmentId);
    return documentId;
}

/**
 * Check whether a driver has already been registered for the given VIN.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param vin VIN of the vehicle to query.
 * @param secondaryOwnerId The secondary owner's person ID.
 * @returns Promise which fulfills with a boolean.
 */
async function isSecondaryOwnerForVehicle(
    txn,
    vin,
    secondaryOwnerId
) {
    console.log(`Finding secondary owners for vehicle with VIN: ${vin}`);
    const query = "SELECT Owners.SecondaryOwners FROM VehicleRegistration AS v WHERE v.VIN = ?";

    const qldbWriter = createQldbWriter();
    util.writeValueAsIon(vin, qldbWriter);
    let doesExist = false;

    await txn.executeInline(query, [qldbWriter]).then((result) => {
        const resultList = result.getResultList();

        resultList.forEach((reader) => {
            const secondaryOwnersList = util.getFieldValue(reader, ["SecondaryOwners"]);

            secondaryOwnersList.forEach((secondaryOwner) => {
                const secondaryOwnerReader = makeReader(JSON.stringify(secondaryOwner));
                if (util.getFieldValue(secondaryOwnerReader, ["PersonId"]) === secondaryOwnerId) {
                    doesExist = true;
                }
            });
        });
    });
    return doesExist;
}

/**
 * Finds and adds secondary owners for a vehicle.
 * @returns Promise which fulfills with void.
 */
var main = async function() {
    let session;
    try {
        session = await createQldbSession();
        const vin = SampleData.VEHICLE_REGISTRATION[1].VIN;
        const govId = SampleData.PERSON[0].GovId;
        await session.executeLambda(async (txn) => {

            const documentId = await getDocumentIdByGovId(txn, govId);

            if (await isSecondaryOwnerForVehicle(txn, vin, documentId)) {
                log(`Person with ID ${documentId} has already been added as a secondary owner of this vehicle.`);
            } else {
                await addSecondaryOwner(txn, vin, documentId);
            }
        }, () => console.log("Retrying due to OCC conflict..."));

        console.log("Secondary owners successfully updated.");
    } catch (e) {
        console.log(`Unable to add secondary owner: ${e}`);
    } finally {
        closeQldbSession(session);
    }
}

if (require.main === module) {
    main();
}