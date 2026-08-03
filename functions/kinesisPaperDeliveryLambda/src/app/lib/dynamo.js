const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  BatchWriteCommand,
  BatchGetCommand,
  GetCommand,
  UpdateCommand,
  DynamoDBDocumentClient,
  TransactWriteCommand
} = require("@aws-sdk/lib-dynamodb");
const {
  getDeliveryWeek,
  buildPaperDeliveryRecord
} = require("./utils");
const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const counterTableName = process.env.KINESIS_PAPERDELIVERY_COUNTERTABLE;
const senderLimitTableName = process.env.KINESIS_PAPERDELIVERY_SENDERLIMITTABLE;
const paperDeliveryTableName = process.env.KINESIS_PAPERDELIVERY_TABLE;
const usedSenderLimitTableName = process.env.KINESIS_PAPERDELIVERY_USEDSENDERLIMITTABLE;
const eventTableName = process.env.KINESIS_PAPERDELIVERY_EVENTTABLE;

const TRANSACTION_INDEX = {
  EVENT_IDEMPOTENCY: 0,
  USED_SENDER_LIMIT: 1,
  PAPER_DELIVERY: 2
};

function calculateTtl(){
  const ttlDays = parseInt(process.env.KINESIS_PAPERDELIVERY_COUNTERTTLDAYS, 10) || 14;
  const expireDate = new Date();
  expireDate.setDate(expireDate.getDate() + ttlDays);
  return Math.floor(expireDate.getTime() / 1000);
}

function retrieveCounterMap(excludeGroupedRecords) {
  const result = {};
  for (const key of Object.keys(excludeGroupedRecords)) {
    const records = excludeGroupedRecords[key];
    const productTypeKey = key.split("~")[1];

    let filteredRecords;

        if (productTypeKey === "RS") {
            filteredRecords = records.filter(
               record => record.entity.communicationType !== "INFORMAL"
            );
        } else {
          filteredRecords = records.filter(
            record => (record.entity.skipSenderLimit || record.entity.attempt && parseInt(record.entity.attempt, 10) === 1) && record.entity.communicationType !== "INFORMAL"
          );
        }

    if (filteredRecords.length > 0) {
      result[key] = filteredRecords.length;
    }
  }
  return result;
}

async function updateExcludeCounter(excludeGroupedRecords, batchItemFailures) {

    const deliveryDate = getDeliveryWeek();
    let ttl = calculateTtl();
    let counterMap = retrieveCounterMap(excludeGroupedRecords);

    for (const [productTypeProvince, inc] of Object.entries(counterMap)) {
      const sk = `EXCLUDE~${productTypeProvince}`;
      try {
        const input = {
            TableName: counterTableName,
            Key: {
              pk: deliveryDate,
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
          const command = new UpdateCommand(input);
          await docClient.send(command);
          console.log(`updateSuccessfully for ${sk}`);
      } catch (error) {
        console.error(`Failed to update counter for sk: ${sk}`, error);
        const failedRecords = excludeGroupedRecords[productTypeProvince];
        if (failedRecords) {
          const failedSeqNumbers = failedRecords.map((i) => { return { itemIdentifier: i.kinesisSeqNumber }; });
          batchItemFailures.push(...failedSeqNumbers);
        }
      }
    }
    return batchItemFailures;
}

async function updateSenderPriorityCounter(groupedSenderPaIdRecords, batchItemFailures) {
  const deliveryDate = getDeliveryWeek();

  for (const [senderPaId, records] of Object.entries(groupedSenderPaIdRecords)) {
    const sk = `SENDER_PRIORITY~${senderPaId}`;
    try {
      const priorities = new Set(
        records
          .map(r => r.entity.senderPriority)
          .filter(p => p !== 0)
      );
      console.log(`Updating sender priority counter for senderPaId: ${senderPaId} with priorities: ${JSON.stringify(Array.from(priorities))}`);

      if (!priorities || priorities.size === 0) {
        console.log(`Skipping updating sender priority for senderPaId: ${senderPaId}`);
        continue;
      }

      const input = {
        TableName: counterTableName,
        Key: {
          pk: deliveryDate,
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
      const failedSeqNumbers = records.map(i => ({ itemIdentifier: i.kinesisSeqNumber }));
      batchItemFailures.push(...failedSeqNumbers);
    }
  }
  return batchItemFailures;
}

async function batchWritePaperDeliveryRecords(paperDeliveryRecords, batchItemFailures) {
  const batch_size = process.env.KINESIS_BATCHSIZE;
  console.log(`Batch size: ${batch_size}`);
  const tableName = paperDeliveryTableName;

  const params = {
        RequestItems: {
          [tableName]: paperDeliveryRecords.map(record => ({
            PutRequest: {
              Item: record.entity
            }
          }))
        }
  }

  try {
    const command = new BatchWriteCommand(params);
    const response = await docClient.send(command);
    console.log(`Batch write successful for ${paperDeliveryRecords.length} items.`);

    const writeRequests = response.UnprocessedItems[tableName];
    if (writeRequests) {
      const failedIDs = [];
      console.log(`Unprocessed items: ${writeRequests.length}`);
      for (const writeRequest of writeRequests) {
        const unprocessedEntity = writeRequest.PutRequest.Item;
        const failedRecord = paperDeliveryRecords.find(record => record.entity.sk === unprocessedEntity.sk);
        if (failedRecord) {
          failedIDs.push(failedRecord.kinesisSeqNumber);
        }
      }
      batchItemFailures = batchItemFailures.concat(failedIDs.map((i) => {
        return { itemIdentifier: i };
      }));
      console.warn("batchItemFailures:" + JSON.stringify(batchItemFailures));
    }
  } catch (error) {
    console.error('Error in batch write:', error);
    batchItemFailures = batchItemFailures.concat(paperDeliveryRecords.map((i) => { return { itemIdentifier: i.kinesisSeqNumber }; }));
  }
  return batchItemFailures;
}

async function batchWriteKinesisEventRecords(eventRecords) {
  if (!eventRecords || eventRecords.length === 0) {
      console.log("No Kinesis event records to write");
      return;
  }

  const params = {
    RequestItems: {
      [eventTableName]: eventRecords.map(record => ({
        PutRequest: { Item: record }
      }))
    }
  };
  const command = new BatchWriteCommand(params);
  return await docClient.send(command);
}

async function batchGetKinesisEventRecords(keys) {
  const params = {
    RequestItems: {
      [eventTableName]: {
        Keys: keys.map(key => (
            {
              requestId: key
            }
        ))
    }
    }
  };
  const command = new BatchGetCommand(params);
  return await docClient.send(command).then(response => {
    const items = response.Responses[tableName];
    if (!items || items.length === 0) {
      return [];
    }
    return items.map(item => item.requestId);
  });
}

async function getSenderLimit(senderPaId, productType, province, notificationSentAtWeek) {
  const key = {
    pk: `${senderPaId}~${productType}~${province}`,
    deliveryDate: notificationSentAtWeek
  };

  const command = new GetCommand({
    TableName: senderLimitTableName,
    Key: key
  });

  const response = await docClient.send(command);
  return response.Item || null;
}

async function updateUsedSenderLimitAndInsertPaperDeliveries(groupRecords, paperDeliveryRecords, notificationSentAtWeek, weeklyEstimate) {
  const deliveryWeek = getDeliveryWeek();
  for (const eventItem of groupRecords) {
    /*
     * Il record inserito nella transazione viene marcato con
     * skipSenderLimit = true perché la scrittura PaperDelivery
     * avviene già all'interno della TransactWrite.
     */
    const transactionalPaperDelivery = {
      entity: buildPaperDeliveryRecord(
        eventItem,
        deliveryWeek,
        true,
        true
      ),
      kinesisSeqNumber: eventItem.kinesisSeqNumber
    };

    const key = {
      pk: `${eventItem.senderPaId}~${eventItem.productType}~${eventItem.recipientNormalizedAddress.pr}`,
      deliveryDate: notificationSentAtWeek
    };

    const eventRecord = {
      requestId: eventItem.requestId,
      ttl: calculateTtl()
    };

    const expressionAttributeNames = {
      "#numberOfShipment": "numberOfShipment",
      "#weeklyEstimate": "weeklyEstimate",
      "#paId": "paId",
      "#productType": "productType",
      "#province": "province"
    };

    const expressionAttributeValues = {
      ":one": 1,
      ":weeklyEstimate": weeklyEstimate,
      ":paId": eventItem.senderPaId,
      ":productType": eventItem.productType,
      ":province": eventItem.recipientNormalizedAddress.pr
    };

    const setExpressions = [
      "#weeklyEstimate = if_not_exists(#weeklyEstimate, :weeklyEstimate)",
      "#paId = if_not_exists(#paId, :paId)",
      "#productType = if_not_exists(#productType, :productType)",
      "#province = if_not_exists(#province, :province)"
    ];

    try {
      await docClient.send(
        new TransactWriteCommand({
          TransactItems: [
            {
              Put: {
                TableName: eventTableName,
                Item: eventRecord,
                ConditionExpression: "attribute_not_exists(requestId)"
              }
            },
            {
              Update: {
                TableName: usedSenderLimitTableName,
                Key: key,
                UpdateExpression:
                  `SET ${setExpressions.join(", ")} ` +
                  "ADD #numberOfShipment :one",
                ConditionExpression:
                  "attribute_not_exists(#numberOfShipment) OR #numberOfShipment < :weeklyEstimate",
                ExpressionAttributeNames: expressionAttributeNames,
                ExpressionAttributeValues: expressionAttributeValues
              }
            },
            {
              Put: {
                TableName: paperDeliveryTableName,
                Item: transactionalPaperDelivery.entity,
                ConditionExpression: 'attribute_not_exists(requestId)',
              }
            }
          ]
        })
      );
      paperDeliveryRecords.push(transactionalPaperDelivery);
    } catch (error) {
      if(isEventTableConditionalCheckFailed(error)) {
        console.log(`Duplicate requestId ${eventItem.requestId} found in event table. Skipping processing.`);
        continue;
      }
      if (isUsedSenderLimitConditionFailure(error)) {
        console.log(`Sender limit reached for delayed PaperDelivery ${eventItem.requestId}`);
        paperDeliveryRecords.push({
          entity: buildPaperDeliveryRecord(
            eventItem,
            deliveryWeek,
            true,
            false
          ),
          kinesisSeqNumber: eventItem.kinesisSeqNumber
        });
        continue;
      }
      console.error(`Failed transactional processing for ${eventItem.requestId}`, error);
      throw error;
    }
  }
}

function isUsedSenderLimitConditionFailure(error) {
  return (
    error?.name === "TransactionCanceledException" &&
    error?.CancellationReasons?.[TRANSACTION_INDEX.USED_SENDER_LIMIT]?.Code === "ConditionalCheckFailed"
  );
}

function isEventTableConditionalCheckFailed(error) {
  return (
    error?.name === "TransactionCanceledException" &&
    error?.CancellationReasons?.[TRANSACTION_INDEX.EVENT_IDEMPOTENCY]?.Code === "ConditionalCheckFailed"
  );
}


module.exports = {
  batchWritePaperDeliveryRecords,
  updateExcludeCounter,
  updateSenderPriorityCounter,
  batchWriteKinesisEventRecords,
  batchGetKinesisEventRecords,
  getSenderLimit,
  updateUsedSenderLimitAndInsertPaperDeliveries
};