const { DynamoDBClient, ScanCommand, BatchWriteItemCommand } = require('@aws-sdk/client-dynamodb');

class AwsDynamoDBClient {
  constructor(credentials, isLocal) {
    const conf = {
      region: 'eu-south-1',
      credentials: credentials,
    };

    if (isLocal) {
      conf.endpoint = 'http://localhost:4566';
      conf.sslEnabled = false;
      conf.region = 'us-east-1';
    }

    this._dynamoClient = new DynamoDBClient(conf);
  }

  async clearTable(tableName) {
    // Scan all items in the table
    let lastEvaluatedKey = undefined;
    do {
      const scanParams = {
      TableName: tableName,
      ExclusiveStartKey: lastEvaluatedKey,
      };
      const scanResult = await this._dynamoClient.send(new ScanCommand(scanParams));
      const items = scanResult.Items || [];

      // DynamoDB batch write supports max 25 items per request
      for (let i = 0; i < items.length; i += 25) {
        const batch = items.slice(i, i + 25);
        console.info(`Writing a batch of size ${batch.length}`);
        const deleteRequests = [];

        batch.forEach((item) => {
          deleteRequests.push({
            DeleteRequest: {
              Key: { pk: item.pk, activationDateFrom: item.activationDateFrom},
            },
          });
        });

        try {
          return await this.retryableBatchWriteItems(tableName, deleteRequests);
        } catch (error) {
          console.error('Error during BatchWriteItemCommand cause=', error);
          process.exit(1);
        }
      }
      lastEvaluatedKey = scanResult.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    console.log(`All items deleted from table ${tableName}.`);
  }


  async groupAndBatchWriteItems(tableName, items, batchSize = 25) {
    for (let i = 0; i < items.length; i = i + batchSize) {
      const batch = items.slice(i, i + batchSize);
      await this.batchWriteItems(tableName, batch);
    }
  }

  async batchWriteItems(tableName, batch) {

    console.info(`Writing a batch of size ${batch.length}`);
    const putRequests = [];

    batch.forEach((element) => {
      putRequests.push({
        PutRequest: {
          Item: element,
        },
      });
    });

    try {
      return await this.retryableBatchWriteItems(tableName, putRequests);
    } catch (error) {
      console.error('Error during BatchWriteItemCommand cause=', error);
      process.exit(1);
    }
  }

  async retryableBatchWriteItems(tableName, requests, retryCount = 0) {
    const retryWait = (ms) => new Promise((res) => setTimeout(res, ms));

    if (retryCount && retryCount > 8)
      throw new Error(`Retry count limit exceeded, current is: ${retryCount}`);

    const input = {
      RequestItems: {
        [tableName]: requests,
      },
      ReturnConsumedCapacity: 'TOTAL',
      ReturnItemCollectionMetrics: 'SIZE',
    };

    const batchWriteResponse = await this._dynamoClient.send(
      new BatchWriteItemCommand(input)
    );

    if (
      batchWriteResponse.UnprocessedItems &&
      batchWriteResponse.UnprocessedItems.length > 0
    ) {
      console.log(
        `Wait and retry unprocessed items for the ${retryCount} time`
      );

      await retryWait(2 ** retryCount * 10); // start with 10 ms waiting
      return this.retryableBatchWriteItems(
        tableName,
        batchWriteResponse.UnprocessedItems,
        retryCount + 1
      );
    }

    return batchWriteResponse;
  }
}

exports.AwsDynamoDBClient = AwsDynamoDBClient;
