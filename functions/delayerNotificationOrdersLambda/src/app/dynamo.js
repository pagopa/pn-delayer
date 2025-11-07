'use strict';
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const NOTIFICATION_ORDERS = process.env.NOTIFICATION_ORDERS_TABLENAME || 'pn-NotificationOrders';

/**
 * Persists order records in DynamoDB using batch write with chunking.
 * @param {Array<Object>} orderRecords
 * @param {string} fileKey
 */
async function persistOrderRecords(orderRecords, fileKey) {
    const CHUNK_SIZE = 25;

    let recordsToProcess = [...orderRecords];
    while (recordsToProcess.length > 0) {
        const chunk = recordsToProcess.splice(0, CHUNK_SIZE);
        const requestItems = buildRequestItems(chunk);
        await batchWriteWithRetry(requestItems);
    }
}
function buildRequestItems(chunk) {
    const ttlValue = Math.floor(Date.now() / 1000) + 60 * 60 * 24 * 365;

    return {
        [NOTIFICATION_ORDERS]: chunk.map(Item => ({
            PutRequest: { Item: { ...Item, ttl: ttlValue } }
        }))
    };
}

/**
 * Helper che esegue BatchWriteCommand e ritenta con back-off esponenziale.
 */
async function batchWriteWithRetry(requestItems, maxRetries = 5) {
    let unprocessed = requestItems;
    let attempt = 0;

    while (unprocessed && Object.keys(unprocessed).length > 0) {
        const resp = await client.send(new BatchWriteCommand({ RequestItems: unprocessed }));
        unprocessed = resp.UnprocessedItems;

        if (!unprocessed || Object.keys(unprocessed).length === 0) {
            break;
        }

        if (attempt >= maxRetries) {
            const err = new Error('Exceeded maxRetries while writing to DynamoDB');
            err.unprocessedItems = unprocessed;
            throw err;
        }

        const delay = Math.floor(Math.pow(2, attempt) * 100 + Math.random() * 100);
        await new Promise(r => setTimeout(r, delay));
    }
}

module.exports = { persistOrderRecords };