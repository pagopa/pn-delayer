const proxyquire = require('proxyquire').noCallThru();
const sinon = require('sinon');
const { expect } = require('chai');

describe('dynamo.js', () => {
  let dynamo;

  let dynamoClientSendStub;
  let DynamoDBClientStub;
  let QueryCommandStub;
  let BatchWriteItemCommandStub;
  let TransactWriteItemsCommandStub;
  let marshallStub;
  let unmarshallStub;
  let chunkArrayStub;

  beforeEach(() => {
    process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME = 'test-table';

    dynamoClientSendStub = sinon.stub();

    DynamoDBClientStub = sinon.stub().returns({
      send: dynamoClientSendStub
    });

    QueryCommandStub = sinon.stub().callsFake(input => input);
    BatchWriteItemCommandStub = sinon.stub().callsFake(input => input);
    TransactWriteItemsCommandStub = sinon.stub().callsFake(input => input);

    marshallStub = sinon.stub().callsFake(value => value);
    unmarshallStub = sinon.stub().callsFake(value => value);

    chunkArrayStub = sinon.stub().callsFake((items, size) => {
      const chunks = [];
      for (let i = 0; i < items.length; i += size) {
        chunks.push(items.slice(i, i + size));
      }
      return chunks;
    });

    dynamo = proxyquire('../app/lib/dynamo', {
      '@aws-sdk/client-dynamodb': {
        DynamoDBClient: DynamoDBClientStub,
        QueryCommand: QueryCommandStub,
        BatchWriteItemCommand: BatchWriteItemCommandStub,
        TransactWriteItemsCommand: TransactWriteItemsCommandStub
      },
      '@aws-sdk/util-dynamodb': {
        marshall: marshallStub,
        unmarshall: unmarshallStub
      },
      './utils': {
        chunkArray: chunkArrayStub
      }
    });
  });

  afterEach(() => {
    sinon.restore();
    delete process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME;
  });

  it('queryLatestByRequestId lancia errore se requestId mancante', async () => {
    try {
      await dynamo.queryLatestByRequestId();
      throw new Error('Expected error not thrown');
    } catch (err) {
      expect(err.message).to.equal('requestId is required');
    }
  });

  it('queryLatestByRequestId ritorna l’item più recente', async () => {
    const marshalledItem = {
      pk: { S: 'pk1' },
      sk: { S: 'sk1' }
    };

    const unmarshalledItem = {
      pk: 'pk1',
      sk: 'sk1',
      requestId: 'request-id-1'
    };

    unmarshallStub.withArgs(marshalledItem).returns(unmarshalledItem);

    dynamoClientSendStub.resolves({
      Items: [marshalledItem]
    });

    const result = await dynamo.queryLatestByRequestId('request-id-1');

    expect(result).to.deep.equal(unmarshalledItem);

    expect(QueryCommandStub.calledOnce).to.equal(true);
    expect(QueryCommandStub.firstCall.args[0]).to.deep.equal({
      TableName: 'test-table',
      IndexName: 'requestId-CreatedAt-index',
      KeyConditionExpression: 'requestId = :requestId',
      ExpressionAttributeValues: {
        ':requestId': 'request-id-1'
      },
      ScanIndexForward: false,
      Limit: 1
    });

    expect(dynamoClientSendStub.calledOnce).to.equal(true);
  });

  it('queryLatestByRequestId ritorna null se non trova item', async () => {
    dynamoClientSendStub.resolves({
      Items: []
    });

    const result = await dynamo.queryLatestByRequestId('request-id-1');

    expect(result).to.equal(null);
  });

  it('getTableName lancia errore se DELAYER_PAPER_DELIVERY_TABLE_NAME mancante', async () => {
    delete process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME;

    try {
      await dynamo.queryLatestByRequestId('request-id-1');
      throw new Error('Expected error not thrown');
    } catch (err) {
      expect(err.message).to.equal('DELAYER_PAPER_DELIVERY_TABLE_NAME not found');
    }
  });

  it('insertItemsBatch gestisce batch write e ritorna [] quando nessun Unprocessed', async () => {
    dynamoClientSendStub.resolves({
      UnprocessedItems: {
        'test-table': []
      }
    });

    const putRequests = [
      {
        PutRequest: {
          Item: {
            id: 1
          }
        }
      }
    ];

    const result = await dynamo.insertItemsBatch(putRequests);

    expect(result).to.deep.equal([]);
    expect(chunkArrayStub.calledWith(putRequests, 25)).to.equal(true);

    expect(BatchWriteItemCommandStub.calledOnce).to.equal(true);
    expect(BatchWriteItemCommandStub.firstCall.args[0]).to.deep.equal({
      RequestItems: {
        'test-table': [
          {
            PutRequest: {
              Item: {
                id: 1
              }
            }
          }
        ]
      }
    });
  });

  it('insertItemsBatch ritorna [] se input vuoto', async () => {
    const result = await dynamo.insertItemsBatch([]);

    expect(result).to.deep.equal([]);
    expect(dynamoClientSendStub.notCalled).to.equal(true);
  });

  it('insertItemsBatch ritenta fino a 3 volte e poi ritorna gli unprocessed', async () => {
    const clock = sinon.useFakeTimers();

    const putRequests = [
      {
        PutRequest: {
          Item: {
            id: 1
          }
        }
      }
    ];

    dynamoClientSendStub.resolves({
      UnprocessedItems: {
        'test-table': putRequests
      }
    });

    const promise = dynamo.insertItemsBatch(putRequests);

    await clock.tickAsync(1000);
    await clock.tickAsync(2000);
    await clock.tickAsync(4000);

    const result = await promise;

    expect(dynamoClientSendStub.callCount).to.equal(4);
    expect(result).to.deep.equal(putRequests);

    clock.restore();
  });

  it('moveItemsToDeleted ritorna senza chiamare DynamoDB se input vuoto', async () => {
    await dynamo.moveItemsToDeleted([]);

    expect(dynamoClientSendStub.notCalled).to.equal(true);
  });

  it('moveItemsToDeleted esegue TransactWriteItems con Delete e Put DELETED', async () => {
    dynamoClientSendStub.resolves({});

    const item = {
      pk: 'pk1',
      sk: 'sk1',
      requestId: 'request-id-1',
      workflowStep: 'EVALUATE_SENDER_LIMIT'
    };

    await dynamo.moveItemsToDeleted([item]);

    expect(chunkArrayStub.calledWith([item], 50)).to.equal(true);
    expect(TransactWriteItemsCommandStub.calledOnce).to.equal(true);

    expect(TransactWriteItemsCommandStub.firstCall.args[0]).to.deep.equal({
      TransactItems: [
        {
          Delete: {
            TableName: 'test-table',
            Key: {
              pk: 'pk1',
              sk: 'sk1'
            }
          }
        },
        {
          Put: {
            TableName: 'test-table',
            Item: {
              pk: 'DELETED#pk1',
              sk: 'sk1',
              requestId: 'request-id-1',
              workflowStep: 'EVALUATE_SENDER_LIMIT'
            }
          }
        }
      ]
    });
  });

  it('moveItemsToDeleted lancia errore se item non ha pk o sk', async () => {
    try {
      await dynamo.moveItemsToDeleted([
        {
          pk: 'pk1',
          requestId: 'request-id-1'
        }
      ]);
      throw new Error('Expected error not thrown');
    } catch (err) {
      expect(err.message).to.equal('Item senza pk/sk: requestId=request-id-1');
    }
  });
});