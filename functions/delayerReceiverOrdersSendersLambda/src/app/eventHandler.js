'use strict';

const { downloadJson } = require('./safeStorage');
const { calculateWeeklyEstimates } = require('./algorithm');
const { getProvinceDistribution, existsSenderLimitByFileKey } = require('./dynamo');

/**
 * Handler per eventi SQS (bacth size 1 consigliata)
 * @param {import('aws-lambda').SQSEvent} event
 */
exports.handleEvent = async (event = {}) => {
    console.info(
        `[HANDLER] ▶︎ Received SQS batch with ${event.Records?.length ?? 0} records`
    );

    const allEstimates = [];

    const records = event.Records ?? [];
    const batchItemFailures = [];

    for (const record of records) {
        try {
            console.debug(`[HANDLER] Raw SQS record: ${JSON.stringify(record)}`);
            const body = JSON.parse(record.body);
            const fileKey = body.key;

            const { Count = 0 } = await existsSenderLimitByFileKey(fileKey);
            if (Count > 0) {
            console.info(`[HANDLER] Duplicato: fileKey "${fileKey}" già presente. Skip`);
                continue;
            }

        console.debug(`[HANDLER] fileKey="${fileKey}"`);

            // 1. Scarica il JSON commessa
            const estimateJson = await downloadJson(fileKey);
            const estimates = await calculateWeeklyEstimates(
                estimateJson,
                region => getProvinceDistribution(region),
                fileKey
            );
            allEstimates.push(...estimates);

        } catch (error) {
            console.error(`[HANDLER] Errore nel record ${record.messageId}:`, error );

            batchItemFailures.push({
                itemIdentifier: record.messageId
            });
        }
    }

    console.info(`[HANDLER] Completed. Processed: ${records.length - batchItemFailures.length}, Failed: ${batchItemFailures.length}`);

    return {
        batchItemFailures
    };
};
