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
const util = require('../seed/util');

/**
 * Query a driver's information using the given ID.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param documentId The unique ID of a document in the Person table.
 * @returns Promise which fulfills with a Reader containing the person.
 */
async function findPersonFromDocumentId(txn, documentId) {
    const query = "SELECT p.* FROM Person AS p BY pid WHERE pid = ?";

    const qldbWriter = createQldbWriter();
    util.writeValueAsIon(documentId, qldbWriter);

    let personId;
    await txn.executeInline(query, [qldbWriter]).then((result) => {
        const resultList = result.getResultList();
        if (resultList.length === 0) {
            throw new Error(`Unable to find person with ID: ${documentId}.`);
        }
        personId = resultList[0];
    });
    return personId;
}

/**
 * Find the primary owner for the given VIN.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param vin The VIN to find primary owner for.
 * @returns Promise which fulfills with a Reader containing the primary owner.
 */
async function findPrimaryOwnerForVehicle(txn, vin) {
    console.log(`Finding primary owner for vehicle with VIN: ${vin}`);
    const query = "SELECT Owners.PrimaryOwner.PersonId FROM VehicleRegistration AS v WHERE v.VIN = ?";
    const vinWriter = createQldbWriter();
    util.writeValueAsIon(vin, vinWriter);

    let documentId;
    await txn.executeInline(query, [vinWriter]).then((result) => {
        const resultList = result.getResultList();
        if (resultList.length === 0) {
            throw new Error(`Unable to retrieve document ID using ${vin}.`);
        }
        documentId = util.getFieldValue(resultList[0], ["PersonId"]);
    });
    return findPersonFromDocumentId(txn, documentId);
}

/**
 * Update the primary owner for a vehicle using the given VIN.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param vin The VIN for the vehicle to operate on.
 * @param documentId New PersonId for the primary owner.
 * @returns Promise which fulfills with void.
 */
async function updateVehicleRegistration(txn, vin, documentId) {
    const statement = "UPDATE VehicleRegistration AS r SET r.Owners.PrimaryOwner.PersonId = ? WHERE r.VIN = ?";

    const personIdWriter = createQldbWriter();
    util.writeValueAsIon(documentId, personIdWriter);

    const vinWriter = createQldbWriter();
    util.writeValueAsIon(vin, vinWriter);

    console.log(`Updating the primary owner for vehicle with VIN: ${vin}...`);
    await txn.executeInline(statement, [personIdWriter, vinWriter]).then((result) => {
        const resultList = result.getResultList();
        if (resultList.length === 0) {
            throw new Error("Unable to transfer vehicle, could not find registration.");
        }
        console.log(`Successfully transferred vehicle with VIN ${vin} to new owner.`);
    });
}

/**
 * Validate the current owner of the given vehicle and transfer its ownership to a new owner in a single transaction.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param vin The VIN of the vehicle to transfer ownership of.
 * @param currentOwner The GovId of the current owner of the vehicle.
 * @param newOwner The GovId of the new owner of the vehicle.
 */
async function validateAndUpdateRegistration(
    txn,
    vin,
    currentOwner,
    newOwner
) {
    const primaryOwner = await findPrimaryOwnerForVehicle(txn, vin);
    if (util.getFieldValue(primaryOwner, ["GovId"]) !== currentOwner) {
        console.log("Incorrect primary owner identified for vehicle, unable to transfer.");
    }
    else {
        const documentId = await util.getDocumentId(txn, 'Person', "GovId", newOwner);
        await updateVehicleRegistration(txn, vin, documentId);
        console.log("Successfully transferred vehicle ownership!");
    }
}

/**
 * Find primary owner for a particular vehicle's VIN.
 * Transfer to another primary owner for a particular vehicle's VIN.
 * @returns Promise which fulfills with void.
 */
var main = async function() {
    let session;
    try {
        session = await createQldbSession();

        const vin = SampleData.VEHICLE[0].VIN;
        const previousOwnerGovId = SampleData.PERSON[0].GovId;
        const newPrimaryOwnerGovId = SampleData.PERSON[1].GovId;

        await session.executeLambda(async (txn) => {
            await validateAndUpdateRegistration(txn, vin, previousOwnerGovId,  newPrimaryOwnerGovId);
        }, () => console.log("Retrying due to OCC conflict..."));
    } catch (e) {
        console.error(`Unable to connect and run queries: ${e}`);
    } finally {
        closeQldbSession(session);
    }
}

if (require.main === module) {
    main();
}