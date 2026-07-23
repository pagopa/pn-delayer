const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');

// AGGIUNTE/MODIFICHE in cima al file
let UpdateCommand;
let LocalDate, DayOfWeek, TemporalAdjusters;

beforeEach(() => {
  mockSend = sinon.stub();

  // Spostare ENV prima del proxyquire: counterTableName viene letto a load-time
  process.env.KINESIS_PAPERDELIVERY_TABLE = 'paperDeliveryTable';
  process.env.KINESIS_PAPERDELIVERY_EVENTTABLE = 'PaperDeliveryKinesisEventTable';
  process.env.KINESIS_PAPERDELIVERY_COUNTERTABLE = 'TestCounterTable';
  process.env.KINESIS_BATCHSIZE = '25';
  process.env.KINESIS_PAPERDELIVERY_COUNTERTTLDAYS = '14';
  process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK = '1';

  BatchWriteCommand = function (params) { this.params = params; };
  BatchGetCommand = function (params) { this.params = params; };
  UpdateCommand = function (params) { this.params = params; };
  GetCommand = function (params) { this.params = params; };

  const DynamoDBDocumentClient = {
    from: sinon.stub().returns({ send: mockSend })
  };

  dynamo = proxyquire('../app/lib/dynamo', {
    '@aws-sdk/client-dynamodb': { DynamoDBClient: function () { } },
    '@aws-sdk/lib-dynamodb': {
      BatchWriteCommand,
      BatchGetCommand,
      GetCommand,
      UpdateCommand,
      DynamoDBDocumentClient
    }
  });
});

describe('updateExcludeCounter', () => {
  let clock;

  before(() => {
    // Imposta la data fissa a Lunedì 6 aprile 2026
    clock = sinon.useFakeTimers(new Date('2026-04-06T00:00:00Z').getTime());
  });

  after(() => {
    clock.restore();
  });

  it('updates counters for RS and non-RS with correct filtering', async () => {
    const excludeGroupedRecords = {
      'MILANO~RS': [
        { entity: { communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-rs-1' },
        { entity: { communicationType: 'INFORMAL' }, kinesisSeqNumber: 'seq-rs-2' }
      ],
      'ROMA~890': [
        { entity: { attempt: '1' }, kinesisSeqNumber: 'seq-2' },
        { entity: { attempt: '1', communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-3' },
        { entity: { attempt: '1', communicationType: 'INFORMAL' }, kinesisSeqNumber: 'seq-4' }
      ]
    };
    mockSend.resolves({});

    const result = await dynamo.updateExcludeCounter(excludeGroupedRecords, []);

    expect(result).to.deep.equal([]);
    expect(mockSend.callCount).to.equal(2);
    expect(mockSend.firstCall.args[0]).to.be.instanceOf(UpdateCommand);
    expect(mockSend.secondCall.args[0]).to.be.instanceOf(UpdateCommand);

    const first = mockSend.firstCall.args[0].params;
    const second = mockSend.secondCall.args[0].params;

    expect(first.TableName).to.equal('TestCounterTable');
    expect(first.Key.pk).to.equal('2026-04-13');
    expect(first.Key.sk).to.equal('EXCLUDE~MILANO~RS');
    expect(first.ExpressionAttributeValues[':inc']).to.equal(1);

    expect(second.Key.sk).to.equal('EXCLUDE~ROMA~890');
    expect(second.ExpressionAttributeValues[':inc']).to.equal(2);
  });

  it('does not call Dynamo when all grouped records are filtered out', async () => {
    const excludeGroupedRecords = {
      'MILANO~RS': [{ entity: { communicationType: 'INFORMAL' }, kinesisSeqNumber: 'seq-rs' }],
      'ROMA~890': [{ entity: { attempt: '0', communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-non-rs' }]
    };

    const result = await dynamo.updateExcludeCounter(excludeGroupedRecords, []);

    expect(result).to.deep.equal([]);
    expect(mockSend.called).to.be.false;
  });

  it('adds only failed group sequence numbers when one update fails and continues others', async () => {
    const excludeGroupedRecords = {
      'MILANO~RS': [
        { entity: { communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-rs-1' },
        { entity: { communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-rs-2' }
      ],
      'ROMA~890': [
        { entity: { attempt: '1', communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-ok-1' }
      ]
    };

    mockSend.onFirstCall().rejects(new Error('update fail'));
    mockSend.onSecondCall().resolves({});

    const result = await dynamo.updateExcludeCounter(excludeGroupedRecords, [{ itemIdentifier: 'already-failed' }]);

    expect(mockSend.callCount).to.equal(2);
    expect(result).to.deep.equal([
      { itemIdentifier: 'already-failed' },
      { itemIdentifier: 'seq-rs-1' },
      { itemIdentifier: 'seq-rs-2' }
    ]);
  });

  it('uses custom ttl days from env', async () => {
    process.env.KINESIS_PAPERDELIVERY_COUNTERTTLDAYS = '2';

    // reload module to re-read env + stubs
    dynamo = proxyquire('../app/lib/dynamo', {
      '@aws-sdk/client-dynamodb': { DynamoDBClient: function () { } },
      '@aws-sdk/lib-dynamodb': {
        BatchWriteCommand,
        BatchGetCommand,
        UpdateCommand,
        DynamoDBDocumentClient: { from: sinon.stub().returns({ send: mockSend }) }
      },
      '@js-joda/core': { LocalDate, DayOfWeek, TemporalAdjusters }
    });

    mockSend.resolves({});

    const now = Math.floor(Date.now() / 1000);
    await dynamo.updateExcludeCounter({
      'MILANO~RS': [{ entity: { communicationType: 'REGISTERED_LETTER' }, kinesisSeqNumber: 'seq1' }]
    }, []);

    const ttl = mockSend.firstCall.args[0].params.ExpressionAttributeValues[':ttl'];
    expect(ttl).to.be.within(now + (2 * 86400) - 2, now + (2 * 86400) + 2);
  });
});

describe('batchWritePaperDeliveryRecords - extra branches', () => {
  it('maps only matching unprocessed sk items to batchItemFailures', async () => {
    const records = [
      { entity: { sk: 'SK#1' }, kinesisSeqNumber: 'seq1' },
      { entity: { sk: 'SK#2' }, kinesisSeqNumber: 'seq2' }
    ];
    mockSend.resolves({
      UnprocessedItems: {
        paperDeliveryTable: [
          { PutRequest: { Item: { sk: { S: 'SK#2' } } } }
        ]
      }
    });

    const result = await dynamo.batchWritePaperDeliveryRecords(records, [{ itemIdentifier: 'existing' }]);

    expect(result).to.deep.equal([
      { itemIdentifier: 'existing' },
      { itemIdentifier: 'seq2' }
    ]);
  });

  it('returns all records as failures when response.UnprocessedItems is missing', async () => {
    const records = [
      { entity: { sk: 'SK#1' }, kinesisSeqNumber: 'seq1' },
      { entity: { sk: 'SK#2' }, kinesisSeqNumber: 'seq2' }
    ];
    mockSend.resolves({}); // provoca accesso a undefined e branch catch

    const result = await dynamo.batchWritePaperDeliveryRecords(records, []);

    expect(result).to.deep.equal([
      { itemIdentifier: 'seq1' },
      { itemIdentifier: 'seq2' }
    ]);
  });

  it('does not append failures when unprocessed array is empty', async () => {
    const records = [{ entity: { sk: 'SK#1' }, kinesisSeqNumber: 'seq1' }];
    mockSend.resolves({
      UnprocessedItems: {
        paperDeliveryTable: []
      }
    });

    const result = await dynamo.batchWritePaperDeliveryRecords(records, []);

    expect(result).to.deep.equal([]);
  });
});

describe('batchWriteKinesisEventRecords - extra branches', () => {
  it('sends correct request mapping', async () => {
    const records = [{ requestId: 'seq1' }, { requestId: 'seq2' }];
    mockSend.resolves({ UnprocessedItems: {} });

    await dynamo.batchWriteKinesisEventRecords(records);

    const cmd = mockSend.firstCall.args[0];
    expect(cmd).to.be.instanceOf(BatchWriteCommand);
    expect(cmd.params).to.deep.equal({
      RequestItems: {
        PaperDeliveryKinesisEventTable: [
          { PutRequest: { Item: { requestId: 'seq1' } } },
          { PutRequest: { Item: { requestId: 'seq2' } } }
        ]
      }
    });
  });

  it('handles empty eventRecords array', async () => {
    mockSend.resolves({ UnprocessedItems: {} });

    const result = await dynamo.batchWriteKinesisEventRecords([]);

    expect(result).to.deep.equal({ UnprocessedItems: {} });
  });
});

describe('batchGetKinesisEventRecords - extra branches', () => {
  it('builds keys request payload correctly', async () => {
    mockSend.resolves({
      Responses: {
        PaperDeliveryKinesisEventTable: [{ requestId: 'seq1' }]
      }
    });

    await dynamo.batchGetKinesisEventRecords(['seq1']);

    const cmd = mockSend.firstCall.args[0];
    expect(cmd).to.be.instanceOf(BatchGetCommand);
    expect(cmd.params).to.deep.equal({
      RequestItems: {
        PaperDeliveryKinesisEventTable: {
          Keys: [{ requestId: 'seq1' }]
        }
      }
    });
  })
});

describe('updateSenderPriorityCounter', () => {
  it('updates sender priority counter correctly', async () => {
    const groupedSenderPaIdRecords = {
      'sender1': [
        { entity: { senderPriority: 30 }, kinesisSeqNumber: 'seq1' },
        { entity: { senderPriority: 40 }, kinesisSeqNumber: 'seq2' }
      ],
      'sender2': [
        { entity: { senderPriority: 60 }, kinesisSeqNumber: 'seq3' }
      ]
    };
    const batchItemFailures = [];
    mockSend.resolves({});

    await dynamo.updateSenderPriorityCounter(groupedSenderPaIdRecords, batchItemFailures);

    expect(mockSend.callCount).to.equal(2);
    const firstCallParams = mockSend.firstCall.args[0].params;
    const secondCallParams = mockSend.secondCall.args[0].params;

    expect(firstCallParams.TableName).to.equal('TestCounterTable');
    expect(firstCallParams.Key.sk).to.equal('SENDER_PRIORITY~sender1');
    expect(firstCallParams.ExpressionAttributeValues[':priorities']).to.deep.equal(new Set([30, 40]));

    expect(secondCallParams.Key.sk).to.equal('SENDER_PRIORITY~sender2');
    expect(secondCallParams.ExpressionAttributeValues[':priorities']).to.deep.equal(new Set([60]));
  });

  it('skips updating when no priorities are present', async () => {
    const groupedSenderPaIdRecords = {
      'sender3': [
        { entity: { senderPriority: 0 }, kinesisSeqNumber: 'seq4' },
        { entity: { senderPriority: 0 }, kinesisSeqNumber: 'seq5' }
      ]
    };
    const batchItemFailures = [];
    mockSend.resolves({});

    await dynamo.updateSenderPriorityCounter(groupedSenderPaIdRecords, batchItemFailures);

    expect(mockSend.callCount).to.equal(0);
  });

  it('handles errors and adds failed sequence numbers to batchItemFailures', async () => {
    const groupedSenderPaIdRecords = {
      'sender4': [
        { entity: { senderPriority: 80 }, kinesisSeqNumber: 'seq6' }
      ]
    };
    const batchItemFailures = [];
    mockSend.rejects(new Error('update fail'));

    await dynamo.updateSenderPriorityCounter(groupedSenderPaIdRecords, batchItemFailures);

    expect(mockSend.callCount).to.equal(1);
    expect(batchItemFailures).to.deep.equal([{ itemIdentifier: 'seq6' }]);
  });
});

describe('getSenderLimit', () => {
  it('returns the item when found in DynamoDB', async () => {
    const item = { pk: 'sender1~RS~MI', sk: '2025-05-19' };
    mockSend.resolves({ Item: item });

    const result = await dynamo.getSenderLimit('sender1', 'RS', 'MI', '2025-05-19');

    expect(result).to.deep.equal(item);
  });

  it('returns null when no item is found for the given key', async () => {
    mockSend.resolves({ Item: undefined });

    const result = await dynamo.getSenderLimit('sender1', 'RS', 'MI', '2025-05-19');

    expect(result).to.be.null;
  });

  it('builds the correct pk concatenating senderPaId, productType and province', async () => {
    mockSend.resolves({ Item: {} });

    await dynamo.getSenderLimit('sender42', '890', 'RM', '2025-06-02');

    const cmd = mockSend.firstCall.args[0];
    expect(cmd).to.be.instanceOf(GetCommand);
    expect(cmd.params.Key.pk).to.equal('sender42~890~RM');
    expect(cmd.params.Key.deliveryDate).to.equal('2025-06-02');
    expect(cmd.params.TableName).to.equal(process.env.KINESIS_PAPERDELIVERY_SENDERLIMITTABLE);
  });

  it('returns null when DynamoDB throws an error', async () => {
    mockSend.rejects(new Error('DynamoDB error'));

    const result = await dynamo.getSenderLimit('sender1', 'RS', 'MI', '2025-05-19');

    expect(result).to.be.null;
  });
});

describe('updateDelayedCounter', () => {
  it('sends an UpdateCommand with correct pk, sk and expression attribute values', async () => {
    mockSend.resolves({});

    await dynamo.updateDelayedCounter('2025-05-26', '2025-05-19T00:00:00Z', 'sender1', 'RS', 'MI', 5, 100);

    expect(mockSend.callCount).to.equal(1);
    const cmd = mockSend.firstCall.args[0];
    expect(cmd).to.be.instanceOf(UpdateCommand);
    expect(cmd.params.TableName).to.equal('TestCounterTable');
    expect(cmd.params.Key.pk).to.equal('2025-05-26');
    expect(cmd.params.Key.sk).to.equal('DELAYED~MI~RS~sender1~2025-05-19T00:00:00Z');
    expect(cmd.params.ExpressionAttributeValues[':numberOfShipments']).to.equal(5);
    expect(cmd.params.ExpressionAttributeValues[':notificationSentAtWeek']).to.equal('2025-05-19T00:00:00Z');
    expect(cmd.params.ExpressionAttributeValues[':weeklyEstimate']).to.equal(100);
  });

  it('uses ADD expression so subsequent calls increment the same counter', async () => {
    mockSend.resolves({});

    await dynamo.updateDelayedCounter('2025-05-26', '2025-05-19', 'sender1', 'RS', 'MI', 3, 100);
    await dynamo.updateDelayedCounter('2025-05-26', '2025-05-19', 'sender1', 'RS', 'MI', 2, 100);

    expect(mockSend.callCount).to.equal(2);
    expect(mockSend.firstCall.args[0].params.UpdateExpression).to.include('ADD');
  });

  it('builds a distinct sk for each unique senderPaId, productType and province combination', async () => {
    mockSend.resolves({});

    await dynamo.updateDelayedCounter('2025-05-26', '2025-05-19T00:00:00Z', 'sender1', 'RS', 'MI', 1, 50);
    await dynamo.updateDelayedCounter('2025-05-26', '2025-05-19T00:00:00Z', 'sender2', '890', 'RM', 1, 50);

    const sk1 = mockSend.firstCall.args[0].params.Key.sk;
    const sk2 = mockSend.secondCall.args[0].params.Key.sk;
    expect(sk1).to.equal('DELAYED~MI~RS~sender1~2025-05-19T00:00:00Z');
    expect(sk2).to.equal('DELAYED~RM~890~sender2~2025-05-19T00:00:00Z');
  });

  it('throws when DynamoDB rejects the update', async () => {
    mockSend.rejects(new Error('update fail'));

    let thrown;
    try {
      await dynamo.updateDelayedCounter('2025-05-26', '2025-05-19', 'sender1', 'RS', 'MI', 5, 100);
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.be.instanceOf(Error);
    expect(thrown.message).to.equal('update fail');
  });
});

