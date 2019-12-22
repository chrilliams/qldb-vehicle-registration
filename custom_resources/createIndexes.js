'use strict';

const response = require('cfn-response-promise');
const { QldbSession, Result, TransactionExecutor } = require('amazon-qldb-driver-nodejs')
const { closeQldbSession, createQldbSession } = require('./QLDBHelpers/ConnectToLedger');

module.exports.handler = async (event,context) => {
  try {

    if (event.RequestType === 'Create') {

      let session;
      try {
          session = await createQldbSession();
          await session.executeLambda(async (txn) => {
            Promise.all([
              createIndex(txn, 'Person', 'GovId'),
              createIndex(txn, 'Vehicle', 'VIN'),
              createIndex(txn, 'VehicleRegistration', 'VIN'),
              createIndex(txn, 'VehicleRegistration', 'LicensePlateNumber'),
              createIndex(txn, 'DriversLicense', 'PersonId'),
              createIndex(txn, 'DriversLicense', 'LicenseNumber')

            ]);
        }, () => log("Retrying due to OCC conflict..."));
      } catch (e) {
          console.log(`Unable to connect: ${e}`);
          throw e;
      } finally {
          closeQldbSession(session);
      }


    }

    var responseData = {'requestType': event.RequestType};
    await response.send(event, context, response.SUCCESS, responseData);
  }
  catch(error) {
    console.log(`catch all error: ${error}`);

    await response.send(event, context, response.FAILED, {'Error': error});

  }

};


async function createIndex(txn, tableName, indexAttribute){
  const statement = `CREATE INDEX on ${tableName} (${indexAttribute})`;
  try {
    const result = await txn.executeInline(statement);
    return result.getResultList().length;
  } catch (e) {
    console.log(`error: ${e}`);
  }
}
