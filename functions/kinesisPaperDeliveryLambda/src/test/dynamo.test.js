const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru().noPreserveCache();

let mockSend;
let BatchWriteCommand;
let BatchGetCommand;
let UpdateCommand;
let GetCommand;
let TransactWriteCommand;
let getDeliveryWeekStub;
let buildPaperDeliveryRecordStub;
let dynamo;

beforeEach(() => {
  mockSend = sinon.stub();

  process.env.KINESIS_PAPERDELIVERY_TABLE = 'paperDeliveryTable';
  process.env.KINESIS_PAPERDELIVERY_EVENTTABLE = 'PaperDeliveryKinesisEventTable';
  process.env.KINESIS_PAPERDELIVERY_COUNTERTABLE = 'TestCounterTable';
  process.env.KINESIS_PAPERDELIVERY_SENDERLIMITTABLE = 'TestSenderLimitTable';
  process.env.KINESIS_PAPERDELIVERY_USEDSENDERLIMITTABLE = 'TestUsedSenderLimitTable';
  process.env.KINESIS_EVENTSRECORDTTLSECONDS = '3600';
  process.env.KINESIS_BATCHSIZE = '25';
  process.env.KINESIS_PAPERDELIVERY_COUNTERTTLDAYS = '14';
  process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK = '1';

  BatchWriteCommand = function (params) { this.params = params; };
  BatchGetCommand = function (params) { this.params = params; };
  UpdateCommand = function (params) { this.params = params; };
  GetCommand = function (params) { this.params = params; };
  TransactWriteCommand = function (params) { this.params = params; };

  getDeliveryWeekStub = sinon.stub().returns('2026-04-13');
  buildPaperDeliveryRecordStub = sinon.stub().callsFake(
    (eventItem, deliveryWeek, delayed, skipSenderLimit) => ({
      pk: `${deliveryWeek}~EVALUATE_SENDER_LIMIT`,
      sk: `${eventItem.recipientNormalizedAddress.pr}~${eventItem.notificationSentAt}~${eventItem.requestId}`,
      requestId: eventItem.requestId,
      senderPaId: eventItem.senderPaId,
      productType: eventItem.productType,
      province: eventItem.recipientNormalizedAddress.pr,
      deliveryDate: deliveryWeek,
      delayed,
      skipSenderLimit
    })
  );

  const DynamoDBDocumentClient = {
    from: sinon.stub().returns({ send: mockSend })
  };

  dynamo = proxyquire('../app/lib/dynamo', {
    '@aws-sdk/client-dynamodb': {
      DynamoDBClient: function () { }
    },
    '@aws-sdk/lib-dynamodb': {
      BatchWriteCommand,
      BatchGetCommand,
      GetCommand,
      TransactWriteCommand,
      UpdateCommand,
      DynamoDBDocumentClient
    },
    './utils': {
      getDeliveryWeek: getDeliveryWeekStub,
      buildPaperDeliveryRecord: buildPaperDeliveryRecordStub
    }
  });
});

afterEach(() => {
  sinon.restore();
});

describe('updateExcludeCounter', () => {
  let clock;

  before(() => {
    clock = sinon.useFakeTimers(new Date('2026-04-06T00:00:00Z').getTime());
  });

  after(() => {
    clock.restore();
  });

  it('counts every non-INFORMAL delivery that skips the sender limit', async () => {
    const excludeGroupedRecords = {
      'MILANO~RS': [
        { entity: { skipSenderLimit: true, communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-rs-1' },
        { entity: { skipSenderLimit: true, communicationType: 'INFORMAL' }, kinesisSeqNumber: 'seq-rs-2' }
      ],
      'ROMA~890': [
        { entity: { skipSenderLimit: true, attempt: '1' }, kinesisSeqNumber: 'seq-2' },
        { entity: { skipSenderLimit: true, attempt: '1', communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-3' },
        { entity: { skipSenderLimit: true, attempt: '1', communicationType: 'INFORMAL' }, kinesisSeqNumber: 'seq-4' }
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

  it('counts delayed deliveries that consumed sender limit', async () => {
    const excludeGroupedRecords = {
      'ROMA~AR': [
        { entity: { skipSenderLimit: true, delayed: true, attempt: '0', communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-late-1' },
        { entity: { skipSenderLimit: false, delayed: false, attempt: '0', communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-current-1' }
      ]
    };
    mockSend.resolves({});

    await dynamo.updateExcludeCounter(excludeGroupedRecords, []);

    expect(mockSend.callCount).to.equal(1);
    const params = mockSend.firstCall.args[0].params;
    expect(params.Key.sk).to.equal('EXCLUDE~ROMA~AR');
    expect(params.ExpressionAttributeValues[':inc']).to.equal(1);
  });

  it('does not call Dynamo when all grouped records are filtered out', async () => {
    const excludeGroupedRecords = {
      'MILANO~RS': [{ entity: { skipSenderLimit: true, communicationType: 'INFORMAL' }, kinesisSeqNumber: 'seq-rs' }],
      'ROMA~890': [{ entity: { skipSenderLimit: false, attempt: '0', communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-non-rs' }]
    };

    const result = await dynamo.updateExcludeCounter(excludeGroupedRecords, []);

    expect(result).to.deep.equal([]);
    expect(mockSend.called).to.be.false;
  });

  it('adds only failed group sequence numbers when one update fails and continues others', async () => {
    const excludeGroupedRecords = {
      'MILANO~RS': [
        { entity: { skipSenderLimit: true, communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-rs-1' },
        { entity: { skipSenderLimit: true, communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-rs-2' }
      ],
      'ROMA~890': [
        { entity: { skipSenderLimit: true, attempt: '1', communicationType: 'LEGAL' }, kinesisSeqNumber: 'seq-ok-1' }
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
    mockSend.resolves({});

    const now = Math.floor(Date.now() / 1000);
    await dynamo.updateExcludeCounter({
      'MILANO~RS': [{ entity: { skipSenderLimit: true, communicationType: 'REGISTERED_LETTER' }, kinesisSeqNumber: 'seq1' }]
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
          { PutRequest: { Item: { sk: 'SK#2' } } }
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
    mockSend.resolves({});

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

  it('returns all records as failures when batch write rejects', async () => {
    const records = [
      { entity: { sk: 'SK#1' }, kinesisSeqNumber: 'seq1' },
      { entity: { sk: 'SK#2' }, kinesisSeqNumber: 'seq2' }
    ];
    mockSend.rejects(new Error('batch write failed'));

    const result = await dynamo.batchWritePaperDeliveryRecords(records, []);

    expect(result).to.deep.equal([
      { itemIdentifier: 'seq1' },
      { itemIdentifier: 'seq2' }
    ]);
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

  it('propagates batch write errors', async () => {
    mockSend.rejects(new Error('batch write failed'));

    let thrown;
    try {
      await dynamo.batchWriteKinesisEventRecords([{ requestId: 'seq1' }]);
    } catch (error) {
      thrown = error;
    }

    expect(thrown).to.be.instanceOf(Error);
    expect(thrown.message).to.equal('batch write failed');
  });
});

describe('batchGetKinesisEventRecords - extra branches', () => {
  it('builds keys request payload correctly', async () => {
    mockSend.resolves({
      Responses: {
        PaperDeliveryKinesisEventTable: [{ requestId: 'seq1' }]
      }
    });

    const result = await dynamo.batchGetKinesisEventRecords(['seq1']);

    const cmd = mockSend.firstCall.args[0];
    expect(cmd).to.be.instanceOf(BatchGetCommand);
    expect(cmd.params).to.deep.equal({
      RequestItems: {
        PaperDeliveryKinesisEventTable: {
          Keys: [{ requestId: 'seq1' }]
        }
      }
    });
    expect(result).to.deep.equal(['seq1']);
  });

  it('returns an empty array when no records are found', async () => {
    mockSend.resolves({
      Responses: {
        PaperDeliveryKinesisEventTable: []
      }
    });

    const result = await dynamo.batchGetKinesisEventRecords(['seq1']);

    expect(result).to.deep.equal([]);
  });

  it('propagates batch get errors', async () => {
    mockSend.rejects(new Error('batch get failed'));

    let thrown;
    try {
      await dynamo.batchGetKinesisEventRecords(['seq1']);
    } catch (error) {
      thrown = error;
    }

    expect(thrown).to.be.instanceOf(Error);
    expect(thrown.message).to.equal('batch get failed');
  });
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

  it('removes duplicate priorities before updating', async () => {
    const groupedSenderPaIdRecords = {
      'sender1': [
        { entity: { senderPriority: 30 }, kinesisSeqNumber: 'seq1' },
        { entity: { senderPriority: 30 }, kinesisSeqNumber: 'seq2' }
      ]
    };
    mockSend.resolves({});

    await dynamo.updateSenderPriorityCounter(groupedSenderPaIdRecords, []);

    const priorities = mockSend.firstCall.args[0].params.ExpressionAttributeValues[':priorities'];
    expect(priorities).to.deep.equal(new Set([30]));
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
  it('gets sender limit with the correct key', async () => {
    mockSend.resolves({
      Item: {
        weeklyEstimate: 10
      }
    });

    const result = await dynamo.getSenderLimit(
      'sender1',
      'AR',
      'RM',
      '2025-05-19'
    );

    expect(result).to.deep.equal({
      weeklyEstimate: 10
    });

    const command = mockSend.firstCall.args[0];
    expect(command).to.be.instanceOf(GetCommand);
    expect(command.params).to.deep.equal({
      TableName: 'TestSenderLimitTable',
      Key: {
        pk: 'sender1~AR~RM',
        deliveryDate: '2025-05-19'
      }
    });
  });

  it('returns null when sender limit does not exist', async () => {
    mockSend.resolves({});

    const result = await dynamo.getSenderLimit(
      'sender1',
      'AR',
      'RM',
      '2025-05-19'
    );

    expect(result).to.equal(null);
  });

  it('propagates technical errors', async () => {
    mockSend.rejects(new Error('DynamoDB unavailable'));

    let thrown;
    try {
      await dynamo.getSenderLimit(
        'sender1',
        'AR',
        'RM',
        '2025-05-19'
      );
    } catch (error) {
      thrown = error;
    }

    expect(thrown).to.be.instanceOf(Error);
    expect(thrown.message).to.equal('DynamoDB unavailable');
  });
});

describe('updateUsedSenderLimitAndInsertPaperDeliveries', () => {
  function buildEvent(overrides = {}) {
    return {
      requestId: 'request1',
      kinesisSeqNumber: 'seq1',
      senderPaId: 'sender1',
      productType: 'AR',
      notificationSentAt: '2025-05-21T12:34:25Z',
      prepareRequestDate: '2025-05-21T12:34:25Z',
      recipientNormalizedAddress: {
        pr: 'RM',
        cap: '00100'
      },
      attempt: '0',
      ...overrides
    };
  }

  it('increments used sender limit and inserts PaperDelivery transactionally', async () => {
    const eventItem = buildEvent();
    const paperDeliveryRecords = [];
    mockSend.resolves({});

    await dynamo.updateUsedSenderLimitAndInsertPaperDeliveries(
      [eventItem],
      paperDeliveryRecords,
      '2025-05-19',
      10
    );

    expect(mockSend.calledOnce).to.equal(true);

    const command = mockSend.firstCall.args[0];
    expect(command).to.be.instanceOf(TransactWriteCommand);

    const transaction = command.params;
    expect(transaction.TransactItems).to.have.lengthOf(3);

    const eventPut = transaction.TransactItems[0].Put;
    expect(eventPut.TableName).to.equal('PaperDeliveryKinesisEventTable');
    expect(eventPut.Item.requestId).to.equal('request1');
    expect(eventPut.ConditionExpression).to.equal('attribute_not_exists(requestId)');

    const update = transaction.TransactItems[1].Update;

    const put = transaction.TransactItems[2].Put;
    expect(put.TableName).to.equal('paperDeliveryTable');
    expect(update.Key).to.deep.equal({
      pk: 'sender1~AR~RM',
      deliveryDate: '2025-05-19'
    });
    expect(update.UpdateExpression).to.equal(
      'SET #weeklyEstimate = if_not_exists(#weeklyEstimate, :weeklyEstimate), #paId = if_not_exists(#paId, :paId), #productType = if_not_exists(#productType, :productType), #province = if_not_exists(#province, :province) ADD #numberOfShipment :one'
    );
    expect(update.ConditionExpression).to.equal(
      'attribute_not_exists(#numberOfShipment) OR #numberOfShipment < :weeklyEstimate'
    );
    expect(update.ExpressionAttributeValues[':one']).to.equal(1);
    expect(update.ExpressionAttributeValues[':weeklyEstimate']).to.equal(10);
    expect(update.ExpressionAttributeValues[':paId']).to.equal('sender1');
    expect(update.ExpressionAttributeValues[':productType']).to.equal('AR');
    expect(update.ExpressionAttributeValues[':province']).to.equal('RM');

    const putTransaction = transaction.TransactItems[2].Put;
    expect(putTransaction.TableName).to.equal('paperDeliveryTable');
    expect(putTransaction).to.not.have.property('ConditionExpression');
    expect(putTransaction.Item.skipSenderLimit).to.equal(true);
    expect(putTransaction.Item.delayed).to.equal(true);

    expect(buildPaperDeliveryRecordStub.calledOnceWithExactly(
      eventItem,
      '2026-04-13',
      true,
      true
    )).to.equal(true);

    expect(paperDeliveryRecords).to.deep.equal([
      {
        entity: put.Item,
        kinesisSeqNumber: 'seq1'
      }
    ]);
  });

  it('uses the weekly estimate received in input in the update condition', async () => {
    const eventItem = buildEvent();
    mockSend.resolves({});

    await dynamo.updateUsedSenderLimitAndInsertPaperDeliveries(
      [eventItem],
      [],
      '2025-05-19',
      7,
      []
    );

    const update = mockSend.firstCall.args[0].params.TransactItems[1].Update;
    expect(update.ConditionExpression).to.equal(
      'attribute_not_exists(#numberOfShipment) OR #numberOfShipment < :weeklyEstimate'
    );
    expect(update.ExpressionAttributeValues[':weeklyEstimate']).to.equal(7);
  });

  it('adds a normal delayed record when sender limit condition fails', async () => {
    const eventItem = buildEvent();
    const paperDeliveryRecords = [];
    const cancellation = Object.assign(new Error('conditional failure'), {
      name: 'TransactionCanceledException',
      CancellationReasons: [
        { Code: 'None' },
        { Code: 'ConditionalCheckFailed' },
        { Code: 'None' }
      ]
    });
    mockSend.rejects(cancellation);

    await dynamo.updateUsedSenderLimitAndInsertPaperDeliveries(
      [eventItem],
      paperDeliveryRecords,
      '2025-05-19',
      10,
      []
    );

    expect(buildPaperDeliveryRecordStub.callCount).to.equal(2);
    expect(buildPaperDeliveryRecordStub.firstCall.calledWithExactly(
      eventItem,
      '2026-04-13',
      true,
      true
    )).to.equal(true);
    expect(buildPaperDeliveryRecordStub.secondCall.calledWithExactly(
      eventItem,
      '2026-04-13',
      true,
      false
    )).to.equal(true);

    expect(paperDeliveryRecords).to.have.lengthOf(1);
    expect(paperDeliveryRecords[0]).to.deep.equal({
      entity: {
        pk: '2026-04-13~EVALUATE_SENDER_LIMIT',
        sk: 'RM~2025-05-21T12:34:25Z~request1',
        requestId: 'request1',
        senderPaId: 'sender1',
        productType: 'AR',
        province: 'RM',
        deliveryDate: '2026-04-13',
        delayed: true,
        skipSenderLimit: false
      },
      kinesisSeqNumber: 'seq1'
    });
  });

  it('does not treat a conditional failure on the Put as sender limit exhaustion', async () => {
    const eventItem = buildEvent();
    const paperDeliveryRecords = [];
    const batchItemFailures = [];

    const cancellation = Object.assign(
      new Error('conditional failure'),
      {
        name: 'TransactionCanceledException',
        CancellationReasons: [
          { Code: 'None' },
          { Code: 'None' },
          { Code: 'ConditionalCheckFailed' }
        ]
      }
    );

    mockSend.rejects(cancellation);

    await dynamo.updateUsedSenderLimitAndInsertPaperDeliveries(
      [eventItem],
      paperDeliveryRecords,
      '2025-05-19',
      10,
      batchItemFailures
    );

    expect(batchItemFailures).to.deep.equal([
      { itemIdentifier: 'seq1' }
    ]);

    expect(paperDeliveryRecords).to.deep.equal([]);
    expect(buildPaperDeliveryRecordStub.calledOnce).to.equal(true);
  });

  it('adds technical transaction errors to batchItemFailures', async () => {
    const eventItem = buildEvent();
    const paperDeliveryRecords = [];
    const batchItemFailures = [];

    mockSend.rejects(new Error('DynamoDB unavailable'));

    await dynamo.updateUsedSenderLimitAndInsertPaperDeliveries(
      [eventItem],
      paperDeliveryRecords,
      '2025-05-19',
      10,
      batchItemFailures
    );

    expect(batchItemFailures).to.deep.equal([
      { itemIdentifier: 'seq1' }
    ]);

    expect(paperDeliveryRecords).to.deep.equal([]);
  });

  it('processes group records one at a time in order', async () => {
    const firstEvent = buildEvent();
    const secondEvent = buildEvent({
      requestId: 'request2',
      kinesisSeqNumber: 'seq2'
    });
    const paperDeliveryRecords = [];

    mockSend.onFirstCall().resolves({});
    mockSend.onSecondCall().resolves({});

    await dynamo.updateUsedSenderLimitAndInsertPaperDeliveries(
      [firstEvent, secondEvent],
      paperDeliveryRecords,
      '2025-05-19',
      10,
      []
    );

    expect(mockSend.callCount).to.equal(2);
    expect(buildPaperDeliveryRecordStub.callCount).to.equal(2);
    expect(buildPaperDeliveryRecordStub.firstCall.args[0].requestId).to.equal('request1');
    expect(buildPaperDeliveryRecordStub.secondCall.args[0].requestId).to.equal('request2');
    expect(paperDeliveryRecords.map(record => record.entity.requestId)).to.deep.equal([
      'request1',
      'request2'
    ]);
  });

  it('continues with following records after a sender limit conditional failure', async () => {
    const firstEvent = buildEvent();
    const secondEvent = buildEvent({
      requestId: 'request2',
      kinesisSeqNumber: 'seq2'
    });
    const paperDeliveryRecords = [];
    const cancellation = Object.assign(new Error('conditional failure'), {
      name: 'TransactionCanceledException',
      CancellationReasons: [
        { Code: 'None' },
        { Code: 'ConditionalCheckFailed' },
        { Code: 'None' }
      ]
    });

    mockSend.onFirstCall().rejects(cancellation);
    mockSend.onSecondCall().resolves({});

    await dynamo.updateUsedSenderLimitAndInsertPaperDeliveries(
      [firstEvent, secondEvent],
      paperDeliveryRecords,
      '2025-05-19',
      10,
      []
    );

    expect(mockSend.callCount).to.equal(2);
    expect(paperDeliveryRecords).to.have.lengthOf(2);
    expect(paperDeliveryRecords[0].entity.requestId).to.equal('request1');
    expect(paperDeliveryRecords[0].entity.skipSenderLimit).to.equal(false);
    expect(paperDeliveryRecords[1].entity.requestId).to.equal('request2');
    expect(paperDeliveryRecords[1].entity.skipSenderLimit).to.equal(true);
  });
});