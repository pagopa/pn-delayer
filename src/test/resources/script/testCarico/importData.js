"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
    DynamoDBDocumentClient,
    BatchWriteCommand,
    UpdateCommand
} = require("@aws-sdk/lib-dynamodb");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { LocalDate, DayOfWeek, TemporalAdjusters } = require("@js-joda/core");

const LOCALSTACK_ENDPOINT = "http://localhost:4566";
const LOCALSTACK_REGION = "us-east-1";
const ddbClient = new DynamoDBClient({
    endpoint: LOCALSTACK_ENDPOINT,
    region: LOCALSTACK_REGION
});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * IMPORT_DATA operation: downloads the CSV and writes rows to DynamoDB.
 * @param {Array<string>} params[fileName]
 * @returns {Promise<{message:string, processed:number}>}

 const ddbClient = new DynamoDBClient({});
 const docClient = DynamoDBDocumentClient.from(ddbClient);
 */
/**
 * IMPORT_DATA operation: riceve il contenuto CSV e scrive le righe su DynamoDB.
 * @param {Array<string>} params[fileName]
 * @param {string|Buffer} csvContent
 * @returns {Promise<{message:string, processed:number}>}
 */
exports.importData = async (params = [], csvContent) => {
    let [paperDeliveryTableName, countersTableName] = params;
    if (!paperDeliveryTableName || !countersTableName) {
        throw new Error("Required parameters must be [paperDeliveryTableName, countersTableName]");
    }
    if (!csvContent) {
        throw new Error("csvContent deve essere fornito come parametro");
    }

    // Crea uno stream dal contenuto CSV
    const stream = typeof csvContent === "string" || Buffer.isBuffer(csvContent)
        ? Readable.from([csvContent])
        : csvContent;

    let processed = 0;
    const itemsBuffer = [];
    const dayOfWeek = 1; // lunedì
    const deliveryWeek = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();
    for await (const record of stream.pipe(csv({ separator: ";" }))) {
        processed += 1;
        const paperDelivery = buildPaperDeliveryRecord(record, deliveryWeek);
        itemsBuffer.push(paperDelivery);
        if (itemsBuffer.length === 25) {
            await processBatch(paperDeliveryTableName, countersTableName, itemsBuffer.splice(0, itemsBuffer.length), deliveryWeek);
        }
    }
    if (itemsBuffer.length) {
        await processBatch(paperDeliveryTableName, countersTableName, itemsBuffer, deliveryWeek);
    }

    console.log("Processed data:", processed);
    return { message: "CSV imported successfully", processed };
};

async function processBatch(paperDeliveryTableName, countersTableName, items, deliveryWeek) {
  const grouped = groupRecordsByProductAndProvince(items);
  await batchWriteItems(paperDeliveryTableName, items);
  await updateExcludeCounter(countersTableName, grouped, deliveryWeek);
}

/**
 * Utility that performs a BatchWriteCommand and retries unprocessed items.
 * @param {Array<Object>} items
 */
async function batchWriteItems(paperDeliveryTableName, items) {
    let unprocessed = items;
    do {
        const chunk = unprocessed.splice(0, 25);
        const command = new BatchWriteCommand({
            RequestItems: {
                [paperDeliveryTableName]: chunk.map((Item) => ({
                    PutRequest: { Item }
                }))
            }
        });
        const response = await docClient.send(command);
        unprocessed = response.UnprocessedItems?.[paperDeliveryTableName]?.map(
            (r) => r.PutRequest.Item
        ) || [];
        if (unprocessed.length) {
            // simple backoff
            await new Promise((r) => setTimeout(r, 200));
        }
    } while (unprocessed.length);
}

function retrieveCounterMap(excludeGroupedRecords) {
  const result = {};
  for (const key of Object.keys(excludeGroupedRecords)) {
    const records = excludeGroupedRecords[key];
    const productTypeKey = key.split("~")[1];

    let filteredRecords;

    if (productTypeKey === "RS") {
      filteredRecords = records;
    } else {
      filteredRecords = records.filter(
        record => record.attempt && parseInt(record.attempt, 10) === 1
      );
    }

    if (filteredRecords.length > 0) {
      result[key] = filteredRecords.length;
    }
  }
  return result;
}

function calculateTtl(){
  const ttlDays = 14;
  const expireDate = new Date();
  expireDate.setDate(expireDate.getDate() + ttlDays);
  return Math.floor(expireDate.getTime() / 1000);
}

async function updateExcludeCounter(countersTableName, excludeGroupedRecords, deliveryWeek) {
    const ttl = calculateTtl();
    const counterMap = retrieveCounterMap(excludeGroupedRecords);
    for (const [productTypeProvince, inc] of Object.entries(counterMap)) {
        const sk = `EXCLUDE~${productTypeProvince}`;
        const input = {
            TableName: countersTableName,
            Key: {
                pk: deliveryWeek,
                sk: sk
            },
            UpdateExpression: 'ADD #numberOfShipments :inc SET #ttl = :ttl',
            ExpressionAttributeNames: {
                '#numberOfShipments': 'numberOfShipments',
                '#ttl': 'ttl'
            },
            ExpressionAttributeValues: {
                ':inc': inc,
                ':ttl': ttl
            }
        };
        try {
            const command = new UpdateCommand(input);
            await docClient.send(command);
            console.log(`Counter updated successfully for ${sk}`);
        } catch (error) {
            console.error(`Failed to update counter for ${sk}`, error);
        }
    }
}

function buildPaperDeliveryRecord(payload, deliveryWeek) {
    let date = retrieveDate(payload);
    return {
        pk: buildPk(deliveryWeek),
        sk: buildSk(payload.province, date, payload.requestId),
        requestId: payload.requestId,
        createdAt: new Date().toISOString(),
        notificationSentAt: payload.notificationSentAt,
        prepareRequestDate: payload.prepareRequestDate,
        productType: payload.productType,
        senderPaId: payload.senderPaId,
        province: payload.province,
        cap: payload.cap,
        attempt: payload.attempt,
        iun: payload.iun,
    };
}

function retrieveDate(payload) {
    if(payload.productType === "RS" || (payload.attempt && parseInt(payload.attempt, 10) === 1)) {
        return payload.prepareRequestDate;
    }else{
        return payload.notificationSentAt;
    }
}

function buildPk(deliveryWeek) {
    return `${deliveryWeek}~EVALUATE_SENDER_LIMIT`;
}

function buildSk(province, date, requestId) {
    return `${province}~${date}~${requestId}`;
}

const groupRecordsByProductAndProvince = (records) => {
  return records.reduce((acc, record) => {
    const key = `${record.province}~${record.productType}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(record);
    return acc;
  }, {});
};

