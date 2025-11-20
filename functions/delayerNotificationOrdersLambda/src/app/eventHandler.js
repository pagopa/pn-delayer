'use strict';

const { downloadJson } = require('./safeStorage');
const { extractDataFromOrder } = require('./utils');

/**
 * Handler for SQS events (batch size 1 recommended)
 * @param {import('aws-lambda').SQSEvent} event
 */
exports.handleEvent = async (event = {}) => {
    console.info(
        `[HANDLER] ▶︎ Received SQS batch with ${event.Records?.length ?? 0} records`
    );

    const allOrders = [];

    const records = event.Records ?? [];
    for (const record of records) {
        console.debug(`[HANDLER] Raw SQS record: ${JSON.stringify(record)}`);
        const body = JSON.parse(record.body);
        const fileKey = body.key;
        console.debug(`[HANDLER] fileKey="${fileKey}"`);

        // 1. Download the order JSON
        const orderJson = await downloadJson(fileKey);
        const orders = await extractDataFromOrder(
            orderJson,
            fileKey
        );
        allOrders.push(...orders);
    }

    return {
        statusCode: 200,
        body: JSON.stringify({ processed: allOrders.length })
    };
}