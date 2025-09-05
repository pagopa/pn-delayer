'use strict';
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, BatchWriteCommand, UpdateCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');

const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const PROVINCE_TABLE = process.env.PROVINCE_TABLE || 'pn-PaperChannelProvince';
const LIMIT_TABLE = process.env.LIMIT_TABLE || 'pn-PaperDeliverySenderLimit';
const COUNTERS_TABLE = process.env.COUNTERS_TABLE || 'pn-PaperDeliveryCounters';

/**
 * Query pn-PaperChannelProvince to get provinces and their percentage distribution for a region.
 * @param {string} region
 * @returns {Promise<Array<{province:string, percentageDistribution:number}>>}
 */
async function getProvinceDistribution(region) {
    const params = {
        TableName: PROVINCE_TABLE,
        KeyConditionExpression: '#pk = :region',
        ExpressionAttributeNames: { '#pk': 'region' },
        ExpressionAttributeValues: { ':region': region }
    };
    const resp = await client.send(new QueryCommand(params));
    return resp.Items || [];
}

/**
 * Insert or update weekly estimates (pn-PaperDeliverySenderLimit).
 * Uses BatchWrite for new items and UpdateCommand for partial‑week adjustments.
 *
 * @param {Array<object>} estimates result of calculateWeeklyEstimates
 */
async function persistWeeklyEstimates(estimates) {
    // Separate puts vs updates
    const newItems = estimates.filter(e => !e.isPartialWeek);
    const partials = estimates.filter(e => e.isPartialWeek);
    const ttlValue = Math.floor(Date.now() / 1000) + 60 * 60 * 24 * 365 // 1y default

    // Batch write 25 items at a time
    const batches = [];
    for (let i = 0; i < newItems.length; i += 25) {
        batches.push(newItems.slice(i, i + 25));
    }
    // Process batches with retry helper
    for (const batch of batches) {
        const RequestItems = {
            [LIMIT_TABLE]: batch.map(item => ({
                PutRequest: {
                    Item: {
                        pk: `${item.paId}~${item.productType}~${item.province}`,
                        deliveryDate: item.deliveryDate,
                        weeklyEstimate: item.weeklyEstimate,
                        monthlyEstimate: item.monthlyEstimate,
                        originalEstimate: item.originalEstimate,
                        paId: item.paId,
                        productType: item.productType,
                        province: item.province,
                        ttl: ttlValue
                    }
                }
            }))
        };
        await batchWriteWithRetry(RequestItems);
    }

  // Update existing rows for partial weeks
  for (const p of partials) {
    const pk = `${p.paId}~${p.productType}~${p.province}`;
    await client.send(
      new UpdateCommand({
        TableName: LIMIT_TABLE,
        Key: { pk, deliveryDate: p.deliveryDate },
        UpdateExpression: 'SET weeklyEstimate = if_not_exists(weeklyEstimate, :zero) + :inc, productType = if_not_exists(productType, :pt), province = if_not_exists(province, :p), paId = if_not_exists(paId, :pId), #ttlAttribute = if_not_exists(#ttlAttribute, :ttlValue)',
        ExpressionAttributeValues: { ':inc': p.weeklyEstimate, ':zero': 0, ':pt': p.productType, ':p': p.province, ':pId': p.paId, ':ttlValue': ttlValue},
        ExpressionAttributeNames:{ '#ttlAttribute' : 'ttl'}
      })
    );
  }

  // Increment counters table
  for (const item of estimates) {
    const counterPk = item.deliveryDate;
    const counterSk = `SUM_ESTIMATES~${item.productType}~${item.province}~${item.lastUpdate}`;
    await client.send(
      new UpdateCommand({
        TableName: COUNTERS_TABLE,
        Key: { pk: counterPk, sk: counterSk },
        UpdateExpression: 'ADD #c :inc',
        ExpressionAttributeNames: { '#c': 'numberOfShipments' },
        ExpressionAttributeValues: { ':inc': item.weeklyEstimate }
      })
    );
  }
}

/**
 * Helper that executes BatchWriteCommand and transparently retries
 * UnprocessedItems with exponential back‑off + jitter as recommended by AWS.
 *
 * @param {Record<string, Array<Object>>} requestItems
 * @param {number} maxRetries
 */
async function batchWriteWithRetry(requestItems, maxRetries = 5) {
    let unprocessed = requestItems;
    let attempt = 0;

    while (unprocessed && Object.keys(unprocessed).length > 0) {
        const resp = await client.send(new BatchWriteCommand({ RequestItems: unprocessed }));
        unprocessed = resp.UnprocessedItems;

        if (!unprocessed || Object.keys(unprocessed).length === 0) {
            break; // done
        }

        if (attempt >= maxRetries) {
            // You may prefer just logging and letting the caller decide
            const err = new Error('Exceeded maxRetries while writing to DynamoDB');
            err.unprocessedItems = unprocessed;
            throw err;
        }

        // Exponential back‑off with full‑jitter (AWS-recommended)
        const delay = Math.floor(Math.pow(2, attempt) * 100 + Math.random() * 100);
        await new Promise(r => setTimeout(r, delay));
        attempt++;
    }
}




module.exports = { getProvinceDistribution, persistWeeklyEstimates };