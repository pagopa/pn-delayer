"use strict";
const { importData } = require('./importData.js');


/***************************************
 *  Handler entrypoint                *
 ***************************************/

const OPERATIONS = {
    IMPORT_DATA: importData,
    // In futuro: OTHER_OP: otherFunction
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