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

const { QldbSession, Result, TransactionExecutor } = require('amazon-qldb-driver-nodejs')

const { decodeUtf8, makePrettyWriter, Reader, Writer } = require('ion-js');

const { closeQldbSession, createQldbSession } = require('../custom_resources/QLDBHelpers/ConnectToLedger');

/**
 * Pretty print the Readers in the provided result list.
 * @param resultList The result list containing the Readers to pretty print.
 */
function prettyPrintResultList(resultList) {
    const writer = makePrettyWriter();
    resultList.forEach((reader) => {
        writer.writeValues(reader);
    });
    console.log(decodeUtf8(writer.getBytes()));
}

/**
 * Scan for all the documents in a table.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param tableName The name of the table to operate on.
 * @returns Promise which fulfills with a {@linkcode Result} object.
 */
async function scanTableForDocuments(txn, tableName) {
    console.log(`Scanning ${tableName}...`);
    const query = `SELECT * FROM ${tableName}`;
    return await txn.executeInline(query).then((result) => {
        return result;
    });
}

/**
 * Retrieve the list of table names.
 * @param session The session to retrieve table names from.
 * @returns Promise which fulfills with a list of table names.
 */
async function scanTables(session) {
    return await session.getTableNames();
}

/**
 * Scan for all the documents in a table.
 * @returns Promise which fulfills with void.
 */
var main = async function() {
    let session;
    try {
        session = await createQldbSession();
        await scanTables(session).then(async (listofTables) => {
            for (const tableName of listofTables) {
                await session.executeLambda(async (txn) => {
                    const result = await scanTableForDocuments(txn, tableName);
                    prettyPrintResultList(result.getResultList());
                });
            }
        }, () => log("Retrying due to OCC conflict..."));
    } catch (e) {
        console.log(`Error displaying documents: ${e}`);
    } finally {
        closeQldbSession(session);
    }
}

if (require.main === module) {
    main();
}


exports.prettyPrintResultList = prettyPrintResultList;