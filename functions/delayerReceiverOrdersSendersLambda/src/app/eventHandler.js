'use strict';

const { downloadJson } = require('./safeStorage');
const { calculateWeeklyEstimates } = require('./algorithm');
const { getProvinceDistribution, persistWeeklyEstimates } = require('./dynamo');

/**
 * Handler per eventi SQS (bacth size 1 consigliata)
 * @param {import('aws-lambda').SQSEvent} event
 */
exports.handleEvent = async (event = {}) => {
    console.info(
        `[HANDLER] ▶︎ Received SQS batch with ${event.Records?.length ?? 0} records`
    );

    const allEstimates = [];

    for (const record of event.Records) {
        console.debug(`[HANDLER] Raw SQS record: ${JSON.stringify(record)}`);
        const body = JSON.parse(record.body);
        const fileKey = body.key;

        console.debug(`[HANDLER] fileKey="${fileKey}"`);

        // 1. Scarica il JSON commessa
        const estimateJson = await downloadJson(fileKey);

        const estimates = await calculateWeeklyEstimates(
            estimateJson,
            region => getProvinceDistribution(region)
        );
        allEstimates.push(...estimates);
    }

    return {
        statusCode: 200,
        body: JSON.stringify({ processed: allEstimates.length })
    };
}
