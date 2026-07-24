"use strict";
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
    DynamoDBDocumentClient,
    BatchWriteCommand,
    UpdateCommand,
    GetCommand
} = require("@aws-sdk/lib-dynamodb");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { LocalDate, DayOfWeek, TemporalAdjusters, Instant, ZoneOffset} = require("@js-joda/core");

const s3Client = new S3Client({});
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * IMPORT_DATA operation: downloads the CSV and writes rows to DynamoDB.
 * @param {Array<string>} params[paperDeliveryTableName, countersTableName, fileName, deliveryWeek]
 * @returns {Promise<{message:string, processed:number}>}
 */
exports.importData = async (params = []) => {
  const BUCKET_NAME = process.env.BUCKET_NAME;
  let [paperDeliveryTableName, countersTableName, senderLimitTableName, fileName, deliveryWeek] = params;

  if (!paperDeliveryTableName || !countersTableName || !senderLimitTableName || !fileName) {
    throw new Error("Required parameters must be [paperDeliveryTableName, countersTableName, senderLimitTableName, fileName]");
  }

  if (!BUCKET_NAME) {
    throw new Error(
      "Environment variable BUCKET_NAME must be defined"
    );
  }

  const { Body } = await s3Client.send(
    new GetObjectCommand({ Bucket: BUCKET_NAME, Key: fileName })
  );

  // Ensure we have a Node.js Readable stream
  const stream = Body instanceof Readable ? Body : Readable.from(Body);

  let processed = 0;
  const itemsBuffer = [];
  const delayedPaperDeliveryList = [];
  const dayOfWeek = 1; //lunedì

  if (!deliveryWeek) {
    deliveryWeek = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(dayOfWeek))).toString();
  }

  const currentWeek = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();

  for await (const record of stream.pipe(csv({ separator: ";" }))) {
    processed += 1;

    const notificationSentAtWeek = calculateNotificationSentAtWeek(record.notificationSentAt, dayOfWeek);
    const inCurrentWeek = notificationSentAtWeek === currentWeek;

    if (inCurrentWeek) {
      const paperDelivery = buildPaperDeliveryRecord(record, deliveryWeek, false);
      itemsBuffer.push(paperDelivery);
    } else {
      console.log(`PaperDelivery ${record.requestId} belongs to a previous notification week`);
      delayedPaperDeliveryList.push({
        ...record,
        notificationSentAtWeek
      });
    }
  }

  if (delayedPaperDeliveryList.length > 0) {
    console.log(`Processing ${delayedPaperDeliveryList.length} delayed records`);
    const groupedDelayedRecords = groupDelayedRecords(delayedPaperDeliveryList);

    for (const [groupKey, groupRecords] of Object.entries(groupedDelayedRecords)) {
      const [notificationSentAtWeek, senderPaId, productType, province] = groupKey.split("~");
      console.log(`Processing delayed group: ${groupKey}`);

      try {
        const senderLimitItem = await getSenderLimit(
          senderLimitTableName,
          senderPaId,
          productType,
          province,
          notificationSentAtWeek
        );

        let hasSenderLimit = senderLimitItem && senderLimitItem.weeklyEstimate > 0;

        /*
        * Il contatore DELAYED viene incrementato solamente
        * quando esiste un sender limit con weeklyEstimate > 0.
        */
        if (hasSenderLimit) {
          console.log(`Found sender limit for ${groupKey}: ${senderLimitItem.weeklyEstimate}`);
          await updateDelayedCounter(
            countersTableName,
            deliveryWeek,
            notificationSentAtWeek,
            senderPaId,
            productType,
            province,
            groupRecords.length,
            senderLimitItem.weeklyEstimate
          );
          console.log(`Updated DELAYED counter for ${groupKey} with ${groupRecords.length} shipments`);
        } else {
          console.log(`No valid sender limit found for delayed group ${groupKey}`);
        }

        /*
        * Tutte le spedizioni conformi proseguono nel flusso comune.
        *
        * delayed è true solamente quando:
        * - la spedizione appartiene a una settimana precedente;
        * - esiste un weeklyEstimate valido.
        */
        for (const eventItem of groupRecords) {
          const paperDelivery = buildPaperDeliveryRecord(eventItem, deliveryWeek, !!hasSenderLimit);
      	  itemsBuffer.push(paperDelivery);
        }
      } catch (error) {
        console.error(error);
        console.error(
          `Failed to process delayed group ${groupKey}`,
          error
        );
      }
    }
  }

  if (itemsBuffer.length > 0) {
    await processBatch(paperDeliveryTableName, countersTableName, itemsBuffer, deliveryWeek);
  }

  console.log("Processed data:", processed);
  return { message: "CSV imported successfully", processed };
};

async function processBatch(paperDeliveryTableName, countersTableName, items, deliveryWeek) {
  const grouped = groupRecordsByProductAndProvince(items);
  const groupedBySenderPaId = groupRecordsBySenderPaId(items);
  await batchWriteItems(paperDeliveryTableName, items);
  await updateExcludeCounter(countersTableName, grouped, deliveryWeek);
  await updateSenderPriorityCounter(countersTableName, groupedBySenderPaId, deliveryWeek);
}

/**
 * Utility that performs a BatchWriteCommand in chunks of max 25 items and retries unprocessed items.
 * Terminates only when there are no more pending items and no failed items to retry.
 * @param {Array<Object>} items
 */
async function batchWriteItems(paperDeliveryTableName, items) {
  const remaining = [...items];

  while (remaining.length > 0) {
    const chunk = remaining.splice(0, 25);
    const command = new BatchWriteCommand({
      RequestItems: {
        [paperDeliveryTableName]: chunk.map((Item) => ({
          PutRequest: { Item }
        }))
      }
    });

    const response = await docClient.send(command);
    const failed = response.UnprocessedItems?.[paperDeliveryTableName]?.map(
      (r) => r.PutRequest.Item
    ) || [];

    if (failed.length > 0) {
      // Rimette i falliti in testa per rielaborarli nel prossimo giro
      remaining.unshift(...failed);
      await new Promise((r) => setTimeout(r, 200)); // simple backoff
    }
  }
}

function retrieveCounterMap(excludeGroupedRecords) {
  const result = {};
  for (const key of Object.keys(excludeGroupedRecords)) {
    const records = excludeGroupedRecords[key];
    const productTypeKey = key.split("~")[1];

    let filteredRecords;

    if (productTypeKey === "RS") {
        filteredRecords = records.filter(
            record => record.communicationType !== "INFORMAL"
          );
    } else {
      filteredRecords = records.filter(
        record => record.attempt && parseInt(record.attempt, 10) === 1 && record.communicationType !== "INFORMAL"
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

async function updateSenderPriorityCounter(countersTableName, groupedSenderPaIdRecords, deliveryWeek) {  
    for (const [senderPaId, records] of Object.entries(groupedSenderPaIdRecords)) {
      const sk = `SENDER_PRIORITY~${senderPaId}`;
      try {
        const priorities = new Set(
          records
            .map(r => r.senderPriority)
            .filter(p => p !== 0)
        );
        console.log(`Updating sender priority counter for senderPaId: ${senderPaId} with priorities: ${JSON.stringify(Array.from(priorities))}`);
  
        if (!priorities || priorities.size === 0) {
          console.log(`Skipping updating sender priority for senderPaId: ${senderPaId}`);
          continue;
        }
  
        const input = {
          TableName: countersTableName,
          Key: {
            pk: deliveryWeek,
            sk: sk
          },
          UpdateExpression: 'ADD #priorities :priorities SET #paId = :paId',
          ExpressionAttributeNames: {
            '#priorities': 'priorities',
            '#paId': 'paId'
          },
          ExpressionAttributeValues: {
            ':priorities': priorities,
            ':paId': senderPaId
          }
        };
        const command = new UpdateCommand(input);
        await docClient.send(command);
        console.log(`updateSuccessfully for ${sk}`);
      } catch (error) {
        console.error(`Failed to update sender priority counter for sk: ${sk}`, error);
      }
    }
  }

  async function getSenderLimit(senderLimitTableName, senderPaId, productType, province, notificationSentAtWeek) {
    const key = {
      pk: `${senderPaId}~${productType}~${province}`,
      deliveryDate: notificationSentAtWeek
    };
  
    try {
      const command = new GetCommand({
        TableName: senderLimitTableName,
        Key: key
      });
      const response = await docClient.send(command);
      return response.Item || null;
    } catch (error) {
      console.error(`Failed to get sender limit for key: ${JSON.stringify(key)}`, error);
      return null;
    }
  }

  async function updateDelayedCounter(countersTableName, deliveryWeek, notificationSentAtWeek, senderPaId, productType, province, numberOfShipments, weeklyEstimate) {
    const sk = `DELAYED~${province}~${productType}~${senderPaId}~${notificationSentAtWeek}`;
  
    const input = {
      TableName: countersTableName,
      Key: {
        pk: deliveryWeek,
        sk: sk
      },
      UpdateExpression: 'ADD #numberOfShipments :numberOfShipments SET #notificationSentAtWeek = :notificationSentAtWeek, #weeklyEstimate = :weeklyEstimate',
      ExpressionAttributeNames: {
        '#numberOfShipments': 'numberOfShipments',
        '#notificationSentAtWeek': 'notificationSentAtWeek',
        '#weeklyEstimate': 'weeklyEstimate'
      },
      ExpressionAttributeValues: {
        ':numberOfShipments': numberOfShipments,
        ':notificationSentAtWeek': notificationSentAtWeek,
        ':weeklyEstimate': weeklyEstimate
      }
    };
  
    try {
      const command = new UpdateCommand(input);
      await docClient.send(command);
      console.log(`Created/updated DELAYED counter for sk: ${sk}`);
    } catch (error) {
      console.error(`Failed to create DELAYED counter for sk: ${sk}`, error);
      throw error;
    }
  }

function buildPaperDeliveryRecord(payload, deliveryWeek, delayed = false) {
  const skipSenderLimit = isRsOrSecondAttempt(payload);
  const date = skipSenderLimit ? payload.prepareRequestDate : payload.notificationSentAt

  const record = {
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
    attempt: parseInt(payload.attempt, 10),
    iun: payload.iun,
    workflowStep: 'EVALUATE_SENDER_LIMIT',
    communicationType: payload.communicationType || 'LEGAL',
    senderPriority: payload.senderPriority ? parseInt(payload.senderPriority, 10) : 0,
    deliveryDate: deliveryWeek,
    delayed: Boolean(delayed),
    skipSenderLimit
  };

  if (payload.senderPaId && !skipSenderLimit) {
    record.senderPaIdOriginalSentAt = `${payload.senderPaId}~${date}`;
  }

  return record;
}

function isRsOrSecondAttempt(payload) {
    return payload.productType === 'RS' || payload.attempt && parseInt(payload.attempt, 10) === 1;
}
function buildPk(deliveryWeek) {
    return `${deliveryWeek}~EVALUATE_SENDER_LIMIT`;
}

function buildSk(province, date, requestId) {
    return `${province}~${date}~${requestId}`;
}

function calculateNotificationSentAtWeek(notificationSentAt, dayOfWeek) {
  return Instant.parse(notificationSentAt)
    .atOffset(ZoneOffset.UTC)
    .toLocalDate()
    .with(
      TemporalAdjusters.previousOrSame(
        DayOfWeek.of(dayOfWeek)
      )
    )
    .toString();
}

function groupRecordsByProductAndProvince(records) {
  return records.reduce((acc, record) => {
    const key = `${record.province}~${record.productType}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(record);
    return acc;
  }, {});
};

function groupRecordsBySenderPaId(records) {
    return records.reduce((acc, record) => {
        const key = record.senderPaId;
        if (!key) {
          return acc;
        }
        if (!acc[key]) {
            acc[key] = [];
        }
        acc[key].push(record);
        return acc;
    }, {});
};

function groupDelayedRecords(records) {
  return records.reduce((acc, record) => {
    const key = `${record.notificationSentAtWeek}~${record.senderPaId}~${record.productType}~${record.province}`;
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(record);
    return acc;
  }, {});
};

