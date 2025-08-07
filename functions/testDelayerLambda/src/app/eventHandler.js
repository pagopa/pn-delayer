"use strict";
const { importData } = require('./importData.js');
const { getUsedCapacity } = require("./getUsedCapacity.js");
const { getDelayerPaperDeliveriesByRequestId } = require("./getDelayerPaperDeliveriesByRequestId.js");
const { runAlgorithm } = require("./runAlgorithm.js");
const { runDelayerToPaperChannelWorkflow } = require("./runDelayerToPaperChannelWorkflow.js");


/***************************************
 *  Handler entrypoint                *
 ***************************************/

const OPERATIONS = {
    IMPORT_DATA: importData,
    GET_USED_CAPACITY: getUsedCapacity,
    GET_BY_REQUEST_ID: getDelayerPaperDeliveriesByRequestId,
    RUN_ALGORITHM: runAlgorithm,
    DELAYER_TO_PAPER_CHANNEL: runDelayerToPaperChannelWorkflow
};

/**
 * @param {Object} event – expects {operationType:string, parameters:Array}
 */
exports.handleEvent = async (event = {}) => {
    const { operationType, parameters } = event;

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