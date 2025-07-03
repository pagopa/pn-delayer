const { DynamoDBClient, BatchWriteItemCommand } = require("@aws-sdk/client-dynamodb");
const { LocalDate, DayOfWeek, TemporalAdjusters, ZoneOffset } = require("@js-joda/core");
const utils = require("./utils");
const {
  DynamoDBDocumentClient,
  QueryCommand,
  UpdateCommand,
  BatchGetCommand
} = require("@aws-sdk/lib-dynamodb");

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const readyToSendTableName = process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME;

async function getItems(priorityKey, executionDate, LastEvaluatedKey, limit) {

  const params = {
    TableName: readyToSendTableName,
    KeyConditionExpression: "priorityKey = :priorityKey AND deliveryDate <= :executionDate",
    ExpressionAttributeValues: {
      ":priorityKey": priorityKey,
      ":executionDate": executionDate,
    },
    ExclusiveStartKey: LastEvaluatedKey || undefined,
    Limit: parseInt(limit, 10)
  };

  const result = await docClient.send(new QueryCommand(params));
  return result || {Items: [], LastEvaluatedKey: {} };
}


async function deleteItems(requestIds, deliveryDate) {
    const deleteRequests = requestIds.map(requestId => ({
            DeleteRequest: { Key: { deliveryDate : {S:deliveryDate}, requestId: {S:requestId} } }
        }));
    return await deleteItemsBatch(deleteRequests, 1);
}

async function deleteItemsBatch(deleteRequests, retryCount) {
    let unprocessedRequests = [];
    const chunks = utils.chunkArray(deleteRequests, 25);
    for (const chunk of chunks) {
        try {
            console.log(`Deleting ${chunk.length} items`);
            let input = {
                RequestItems: {
                    [readyToSendTableName] : chunk }
            };
            const result = await client.send(new BatchWriteItemCommand(input));
            unprocessedRequests.push(
                ...(result.UnprocessedItems?.[readyToSendTableName] || [])
            );
        } catch (error) {
            console.error("Error during batch delete:", error);
            unprocessedRequests.push(...(chunk))
        }
    }
    if (unprocessedRequests.length > 0 && retryCount < 3) {
        console.log(`Retrying ${unprocessedRequests.length} unprocessed items`);
        return deleteItemsBatch(unprocessedRequests, retryCount + 1);
    }

    if (retryCount >= 3 && unprocessedRequests.length > 0) {
        console.error(`Failed to delete ${unprocessedRequests.length} items after 3 attempts`);
        return unprocessedRequests;
    } else {
        console.log("All items deleted successfully.");
        return unprocessedRequests;
    }
}

async function getUsedPrintCapacities() {
  const tableName = process.env.PAPER_DELIVERY_PRINTCAPACITYCOUNTER_TABLENAME;
  const now = LocalDate.now();
  const today = now.toString();
  const dayOfWeek = parseInt(process.env.PN_DELAYER_DELIVERYDATEDAYOFWEEK, 10) || 1;
  const initialDayOfWeek = now.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek))).toString();

  const keys = [
    { pk: today, sk: "DAY" },
    { pk: initialDayOfWeek, sk: "WEEK" }
  ];

  const params = {
    RequestItems: {
      [tableName]: {
        Keys: keys
      }
    }
  };

  const command = new BatchGetCommand(params);
  const response = await docClient.send(command);

  const items = response.Responses?.[tableName] || [];
  const result = {
    daily: items.find(item => item.pk === today && item.sk === "DAY") || null,
    weekly: items.find(item => item.pk === initialDayOfWeek && item.sk === "WEEK") || null
  };

  return result;
}

async function getPrintCapacity() {
  const tableName = process.env.PAPER_DELIVERY_PRINTCAPACITY_TABLENAME;
  const params = {
    TableName: tableName,
    KeyConditionExpression: "pk = :pk",
    ExpressionAttributeValues: {
      ":pk": "PRINT"
    },
    Limit: 1,
    ScanIndexForward: false
  };

  const command = new QueryCommand(params);
  const response = await docClient.send(command);
  return response.Items || [];
}

async function updatePrintCapacityCounter(date, sk, increment) {
  const tableName = process.env.PAPER_DELIVERY_PRINTCAPACITYCOUNTER_TABLENAME;
  const ttl = calculateTtl();
  const params = {
    TableName: tableName,
    Key: { pk: date, sk: sk },
    UpdateExpression: 'ADD #counter :inc SET #ttl = :ttl',
    ExpressionAttributeNames: {
      '#counter': 'counter',
      '#ttl': 'ttl'
    },
    ExpressionAttributeValues: {
      ':inc': increment,
      ':ttl': ttl
    }
  };

  try {
    await docClient.send(new UpdateCommand(params));
    console.log(`Incremented print capacity counter for ${date} (${sk}) by ${increment}`);
  } catch (error) {
    console.error("Error incrementing print capacity counter:", error);
    throw error;
  }
}

function calculateTtl() {
  const ttlDays = parseInt(process.env.PAPER_DELIVERY_PRINTCAPACITYCOUNTER_TTLDAYS, 10) || 60;
  const now = LocalDate.now();
  const expireDate = now.plusDays(ttlDays).atStartOfDay(ZoneOffset.UTC).toEpochSecond();
  return expireDate;
}

module.exports = { getItems , deleteItems, getUsedPrintCapacities, getPrintCapacity, updatePrintCapacityCounter };