'use strict';
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, BatchWriteCommand, UpdateCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');

const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const PROVINCE_TABLE = process.env.PROVINCE_TABLE || 'pn-PaperChannelProvince';
const LIMIT_TABLE = process.env.LIMIT_TABLE || 'pn-PaperDeliverySenderLimit';
const COUNTERS_TABLE = process.env.COUNTERS_TABLE || 'pn-PaperDeliveryCounters';
const FILEKEY_GSI = "fileKey-index";
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
 * Query pn-PaperDeliverySenderLimit to verify if items with same fileKey exists.
 * @param {string} fileKey
 * @returns {Promise<{Count: number}>} Count of items with the given fileKey
 */
async function existsSenderLimitByFileKey(fileKey) {
    const params = {
     TableName: LIMIT_TABLE,
     IndexName: FILEKEY_GSI,
     KeyConditionExpression: "#fk = :fk",
     ExpressionAttributeNames: { "#fk": "fileKey" },
     ExpressionAttributeValues: { ":fk": fileKey },
     Limit: 1,
     Select: "COUNT",
    };

    return await client.send(new QueryCommand(params));
}

/**
 * Insert or update weekly estimates (pn-PaperDeliverySenderLimit).
 * Uses BatchWrite for new items and UpdateCommand for partial‑week adjustments.
 *
 * @param {Array<object>} estimates result of calculateWeeklyEstimates
 * @param fileKey fileKey of Safe Storage
 */
async function persistWeeklyEstimates(estimates, fileKey) {
    // Separate puts vs updates
    const fulls          = estimates.filter(e => e.weekType === 'FULL');
    const partialStarts  = estimates.filter(e => e.weekType === 'PARTIAL_START');
    const partialEnds    = estimates.filter(e => e.weekType === 'PARTIAL_END');

    const ttlValue = Math.floor(Date.now() / 1000) + 60 * 60 * 24 * 365; // 1y
    // Batch write 25 items at a time
    if (fulls.length) {
        const batches = [];
        for (let i = 0; i < fulls.length; i += 25) {
          batches.push(fulls.slice(i, i + 25));
        }

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
                      fileKey: fileKey,
                      ttl: ttlValue
                  }
              }
            }))
          };
          await batchWriteWithRetry(RequestItems);
        }
    }

    // === PARTIAL_START: usa secondWeekWeeklyEstimate ===
    for (const p of partialStarts) {
      await upsertPartial(p, ttlValue, fileKey, true);
    }

    // === PARTIAL_END: usa firstWeekWeeklyEstimate ===
    for (const p of partialEnds) {
      await upsertPartial(p, ttlValue, fileKey, false);
    }

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

// Helper per gestire PARTIAL_START / PARTIAL_END
  async function upsertPartial(p, ttlValue, fileKey, isStart) {
    const pk = `${p.paId}~${p.productType}~${p.province}`;
    const portionAttr = isStart ? 'secondWeekWeeklyEstimate' : 'firstWeekWeeklyEstimate';
    const otherPortionAttr = isStart ? 'firstWeekWeeklyEstimate' : 'secondWeekWeeklyEstimate';

    await client.send(new UpdateCommand({
      TableName: LIMIT_TABLE,
      Key: { pk, deliveryDate: p.deliveryDate },
      UpdateExpression: [
        'SET weeklyEstimate = if_not_exists(#otherWeekPortion, :zero) + :portion,',
        '#portion = :portion,',
        'productType      = if_not_exists(productType, :pt),',
        'province         = if_not_exists(province, :pr),',
        'paId             = if_not_exists(paId, :pa),',
        'fileKey          = if_not_exists(fileKey, :fk),',
        '#ttl             = if_not_exists(#ttl, :ttl) '
      ].join(' '),
      ExpressionAttributeNames: {
        '#portion': portionAttr,
        '#otherWeekPortion': otherPortionAttr,
        '#ttl': 'ttl'
      },
      ExpressionAttributeValues: {
        ':zero': 0,
        ':portion': p.weeklyEstimate,
        ':pt': p.productType,
        ':pr': p.province,
        ':pa': p.paId,
        ':fk': fileKey,
        ':ttl': ttlValue
      }
    }));
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




module.exports = { getProvinceDistribution, persistWeeklyEstimates, existsSenderLimitByFileKey };