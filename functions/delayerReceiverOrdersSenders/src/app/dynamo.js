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

    // Batch write 25 items at a time
    const batches = [];
    for (let i = 0; i < newItems.length; i += 25) {
        batches.push(newItems.slice(i, i + 25));
    }
    for (const batch of batches) {
        const RequestItems = {
            [LIMIT_TABLE]: batch.map(item => ({
                PutRequest: {
                    Item: {
            pk: `${item.paId}~${item.productType}~\${item.province}`,
            deliveryDate: item.deliveryDate,
            weeklyEstimate: item.weeklyEstimate,
            monthlyEstimate: item.monthlyEstimate,
            percentageLimit: 0,
            paId: item.paId,
            productType: item.productType,
            province: item.province,
            ttl: Math.floor(Date.now() / 1000) + 60 * 60 * 24 * 365 // 1y default
          }
        }
      }))
    };
    await client.send(new BatchWriteCommand({ RequestItems }));
  }

  // Update existing rows for partial weeks
  for (const p of partials) {
    const pk = `${p.paId}~${p.productType}~${p.province}`;
    await client.send(
      new UpdateCommand({
        TableName: LIMIT_TABLE,
        Key: { pk, deliveryDate: p.deliveryDate },
        UpdateExpression: 'SET weeklyEstimate = if_not_exists(weeklyEstimate, :zero) + :inc',
        ExpressionAttributeValues: { ':inc': p.weeklyEstimate, ':zero': 0 }
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
        ExpressionAttributeNames: { '#c': 'counter' },
        ExpressionAttributeValues: { ':inc': item.weeklyEstimate }
      })
    );
  }
}

module.exports = { getProvinceDistribution, persistWeeklyEstimates };
