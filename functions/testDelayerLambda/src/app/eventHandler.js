"use strict";
const { importData } = require('./importData.js');
const { deleteData } = require('./deleteData.js');
const { getUsedCapacity } = require("./getUsedCapacity.js");
const { getDelayerPaperDeliveriesByRequestId } = require("./getDelayerPaperDeliveriesByRequestId.js");
const { runAlgorithm } = require("./runAlgorithm.js");
const { runDelayerToPaperChannelWorkflow } = require("./runDelayerToPaperChannelWorkflow.js");
const { getPresignedUrl } = require("./getPresignedUrl.js");
const { getSenderLimit } = require("./getSenderLimit.js");
const { getPaperDelivery } = require("./getPaperDelivery.js");
const { getStatusExecution } = require("./getStatusExecution.js");
const { insertMockCapacities } = require('./insertMockCapacities.js');
const { getDeclaredCapacity } = require("./getDeclaredCapacity.js");
const { getPrintCounter } = require("./getPrintCapacityCounter.js");


/***************************************
 *  Handler entrypoint                *
 ***************************************/

const OPERATIONS = {
    IMPORT_DATA: importData,
    DELETE_DATA: deleteData,
    GET_USED_CAPACITY: getUsedCapacity,
    GET_BY_REQUEST_ID: getDelayerPaperDeliveriesByRequestId,
    RUN_ALGORITHM: runAlgorithm,
    DELAYER_TO_PAPER_CHANNEL: runDelayerToPaperChannelWorkflow,
    GET_PAPER_DELIVERY: getPaperDelivery,
    GET_SENDER_LIMIT: getSenderLimit,
    GET_PRESIGNED_URL: getPresignedUrl,
    GET_STATUS_EXECUTION: getStatusExecution,
    INSERT_MOCK_CAPACITIES: insertMockCapacities,
    GET_DECLARED_CAPACITY: getDeclaredCapacity,
    GET_PRINT_CAPACITY_COUNTER: getPrintCounter
};

/**
 * @param {Object} event – expects {operationType:string, parameters:Array}
 */
exports.handleEvent = async (event = {}) => {
    const { operationType, parameters } = event;

    console.log("handleEvent input:", {
        operationType,
        parameters,
    });

    const operation = OPERATIONS[operationType];
    if (!operation) {
        return {
            statusCode: 400,
            body: JSON.stringify({ message: `Unsupported operationType: ${operationType}` }),
        };
    }

    try {
        const result = await operation(parameters || []);
        return { statusCode: 200, body: JSON.stringify(result) };
    } catch (err) {
        console.error("Operation failed", err);
        return { statusCode: 500, body: JSON.stringify({ message: err.message }) };
    }
};