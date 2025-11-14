const { expect } = require('chai');
const { mockClient } = require('aws-sdk-client-mock');
const { DynamoDBDocumentClient, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
const { persistOrderRecords } = require('../app/dynamo');

const ddbMock = mockClient(DynamoDBDocumentClient);

describe('persistOrderRecords', () => {
  beforeEach(() => {
    ddbMock.reset();
    ddbMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} });
    process.env.TTL_DAYS = '3650';
  });

  it('build request items con TTL corretto', async () => {
    const records = [{ pk: '2025-10-01', sk: 'ente_AR_NZ', value: 100 }];
    await persistOrderRecords(records, 'fileKey');
    const calls = ddbMock.commandCalls(BatchWriteCommand);
    expect(calls.length).to.equal(1);
    const sentItems = calls[0].args[0].input.RequestItems;
    const item = sentItems['pn-NotificationOrders'][0].PutRequest.Item;
    const itemTtlDate = new Date(item.ttl * 1000);
    const expectedYear = new Date().getFullYear() + 10;
    expect(itemTtlDate.getFullYear()).to.equal(expectedYear);
  });

  it('scrive i record in batch su DynamoDB (caso base)', async () => {
    const records = [{ pk: '2025-10-01', sk: 'ente_AR_NZ', value: 100 }];
    await persistOrderRecords(records, 'fileKey');
    const calls = ddbMock.commandCalls(BatchWriteCommand);
    expect(calls.length).to.equal(1);
  });

  it('scrive più chunk se >25 record', async () => {
    const records = Array.from({ length: 60 }, (_, i) => ({ pk: `pk${i}`, sk: `sk${i}`, value: i }));
    await persistOrderRecords(records, 'fileKey');
    const calls = ddbMock.commandCalls(BatchWriteCommand);
    expect(calls.length).to.equal(3);
  });

  it('ritenta se ci sono UnprocessedItems', async () => {
    ddbMock.on(BatchWriteCommand)
      .resolvesOnce({ UnprocessedItems: { table: [{ PutRequest: { Item: { pk: 'retry' } } }] } })
      .resolves({ UnprocessedItems: {} });
    const records = [{ pk: 'retry', sk: 'sk1', value: 1 }];
    await persistOrderRecords(records, 'fileKey');
    const calls = ddbMock.commandCalls(BatchWriteCommand);
    expect(calls.length).to.equal(2);
  });

  it('lancia errore se supera maxRetries', async () => {
    ddbMock.on(BatchWriteCommand).resolves({ UnprocessedItems: { table: [{ PutRequest: { Item: { pk: 'fail' } } }] } });
    const records = [{ pk: 'fail', sk: 'sk1', value: 1 }];
    try {
      await persistOrderRecords(records, 'fileKey');
      throw new Error('Should have thrown');
    } catch (err) {
      expect(err.message).to.include('Exceeded maxRetries');
      const calls = ddbMock.commandCalls(BatchWriteCommand);
      expect(calls.length).to.equal(6);
    }
  });
});
