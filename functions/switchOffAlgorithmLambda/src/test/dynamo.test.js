const proxyquire = require('proxyquire').noCallThru();
const sinon = require('sinon');
const { expect } = require('chai');

describe('dynamo.js', () => {
  let dynamo;
  let docClientStub;
  let DynamoDBClientStub;
  let DynamoDBDocumentClientFromStub;
  let QueryCommandStub;
  let BatchWriteCommandStub;
  let chunkArrayStub;

  beforeEach(() => {
    process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME = 'test-table';
    process.env.QUERY_LIMIT = '1000';

    docClientStub = { send: sinon.stub() };

    DynamoDBClientStub = sinon.stub().returns({});

    DynamoDBDocumentClientFromStub = sinon.stub().returns(docClientStub);
    QueryCommandStub = sinon.stub().callsFake((input) => input);
    BatchWriteCommandStub = sinon.stub().callsFake((input) => input);

    chunkArrayStub = sinon.stub().callsFake((arr) => [arr]);

    dynamo = proxyquire('../app/lib/dynamo', {
      '@aws-sdk/client-dynamodb': {
        DynamoDBClient: DynamoDBClientStub,
      },
      '@aws-sdk/lib-dynamodb': {
        DynamoDBDocumentClient: { from: DynamoDBDocumentClientFromStub },
        QueryCommand: QueryCommandStub,
        BatchWriteCommand: BatchWriteCommandStub,
      },
      './utils': {
        chunkArray: chunkArrayStub,
      },
    });
  });

  afterEach(() => {
    sinon.restore();
    delete process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME;
    delete process.env.QUERY_LIMIT;
  });

  it('queryByPartitionKey ritorna items e lastEvaluatedKey', async () => {
    docClientStub.send.resolves({
      Items: [{ id: 1 }],
      LastEvaluatedKey: { pk: 'next' },
    });

    const result = await dynamo.queryByPartitionKey('pk1', 10);

    expect(result.items).to.deep.equal([{ id: 1 }]);
    expect(result.lastEvaluatedKey).to.deep.equal({ pk: 'next' });

    expect(docClientStub.send.calledOnce).to.equal(true);
    const sentInput = docClientStub.send.firstCall.args[0];

    expect(sentInput).to.deep.equal({
      TableName: 'test-table',
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': 'pk1' },
      Limit: 10, // min(QUERY_LIMIT=1000, executionLimit=10)
    });
  });

  it('queryByPartitionKey include ExclusiveStartKey quando passato', async () => {
    docClientStub.send.resolves({
      Items: [{ id: 2 }],
      LastEvaluatedKey: undefined,
    });

    const lek = { pk: 'prev' };
    const result = await dynamo.queryByPartitionKey('pk1', 10, lek);

    expect(result.items).to.deep.equal([{ id: 2 }]);
    expect(result.lastEvaluatedKey).to.equal(undefined);

    const sentInput = docClientStub.send.firstCall.args[0];
    expect(sentInput).to.include({
      TableName: 'test-table',
      Limit: 10,
    });
    expect(sentInput.ExclusiveStartKey).to.deep.equal(lek);
  });

  it('queryByPartitionKey limita a QUERY_LIMIT quando executionLimit è più alto', async () => {
    process.env.QUERY_LIMIT = '5';
    dynamo = proxyquire('../app/lib/dynamo', {
      '@aws-sdk/client-dynamodb': { DynamoDBClient: DynamoDBClientStub },
      '@aws-sdk/lib-dynamodb': {
        DynamoDBDocumentClient: { from: DynamoDBDocumentClientFromStub },
        QueryCommand: QueryCommandStub,
        BatchWriteCommand: BatchWriteCommandStub,
      },
      './utils': { chunkArray: chunkArrayStub },
    });

    docClientStub.send.resolves({ Items: [], LastEvaluatedKey: undefined });

    await dynamo.queryByPartitionKey('pk1', 999);

    const sentInput = docClientStub.send.firstCall.args[0];
    expect(sentInput.Limit).to.equal(5);
  });

  it('insertItemsBatch gestisce batch write e ritorna [] quando nessun Unprocessed', async () => {
    docClientStub.send.resolves({ UnprocessedItems: { 'test-table': [] } });

    const putRequests = [{ PutRequest: { Item: { id: 1 } } }];
    const result = await dynamo.insertItemsBatch(putRequests);

    expect(result).to.deep.equal([]);

    expect(chunkArrayStub.calledWith(putRequests, 25)).to.equal(true);

    expect(docClientStub.send.calledOnce).to.equal(true);
    const sentInput = docClientStub.send.firstCall.args[0];
    expect(sentInput).to.deep.equal({
      RequestItems: { 'test-table': putRequests },
    });
  });

  it('insertItemsBatch ritenta fino a 3 volte e poi ritorna gli unprocessed', async () => {
    docClientStub.send.resolves({
      UnprocessedItems: { 'test-table': [{ PutRequest: { Item: { id: 1 } } }] },
    });

    const putRequests = [{ PutRequest: { Item: { id: 1 } } }];
    const result = await dynamo.insertItemsBatch(putRequests);

    expect(docClientStub.send.callCount).to.equal(4);
    expect(result).to.deep.equal([{ PutRequest: { Item: { id: 1 } } }]);
  });

  it('insertItemsBatch in caso di errore aggiunge il chunk agli unprocessed', async () => {
    docClientStub.send.rejects(new Error('boom'));

    const putRequests = [{ PutRequest: { Item: { id: 1 } } }];
    const result = await dynamo.insertItemsBatch(putRequests);

    expect(docClientStub.send.callCount).to.equal(4);
    expect(result).to.deep.equal(putRequests);
  });
});
