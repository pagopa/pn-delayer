'use strict';

const { downloadJson } = require('./safeStorage');
const { calculateWeeklyEstimates } = require('./algorithm');
const { getProvinceDistribution, persistWeeklyEstimates } = require('./dynamo');

/**
 * Handler per eventi SQS (bacth size 1 consigliata)
 * @param {import('aws-lambda').SQSEvent} event
 */
exports.handleEvent = async (event = {}) => {
    for (const record of event.Records) {
        const body = JSON.parse(record.body);
        const fileKey = body.key;
        const lastUpdate = body.lastUpdate;

        // 1. Scarica il JSON commessa
        const commessaJson = await downloadJson(fileKey);

        const estimates = await calculateWeeklyEstimates(
            commessaJson,
            region => getProvinceDistribution(region)
        );
        allEstimates.push(...estimates);
    }

    if (allEstimates.length > 0) {
        await persistWeeklyEstimates(allEstimates);
    }

    return {
        statusCode: 200,
        body: JSON.stringify({ processed: allEstimates.length })
    };
}
