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

    BatchWriteCommand = function(params) { this.params = params; };
    BatchGetCommand = function(params) { this.params = params; };
    UpdateCommand = function(params) { this.params = params; };

    const DynamoDBDocumentClient = {
      from: sinon.stub().returns({ send: mockSend })
    };

    const withStub = sinon.stub().returns({ toString: () => '2026-04-06' });
    LocalDate = { now: sinon.stub().returns({ with: withStub }) };
    DayOfWeek = { of: sinon.stub().callsFake((d) => `DOW_${d}`) };
    TemporalAdjusters = { next: sinon.stub().callsFake((d) => `NEXT_${d}`) };

    dynamo = proxyquire('../app/lib/dynamo', {
      '@aws-sdk/client-dynamodb': { DynamoDBClient: function() {} },
      '@aws-sdk/lib-dynamodb': {
        BatchWriteCommand,
        BatchGetCommand,
        UpdateCommand,
        DynamoDBDocumentClient
      },
      '@js-joda/core': {
        LocalDate,
        DayOfWeek,
        TemporalAdjusters
      }
    });
  });

describe('updateExcludeCounter', () => {
    it('updates counters for RS and non-RS with correct filtering', async () => {
      const excludeGroupedRecords = {
        'MILANO~RS': [
          { communicationType: 'LEGAL', kinesisSeqNumber: 'seq-rs-1' },
          { communicationType: 'INFORMAL', kinesisSeqNumber: 'seq-rs-2' }
        ],
        'ROMA~890': [
          { attempt: '1',  kinesisSeqNumber: 'seq-2' },
          { attempt: '1',  communicationType: 'LEGAL', kinesisSeqNumber: 'seq-3' },
          { attempt: '1', communicationType: 'INFORMAL', kinesisSeqNumber: 'seq-4' }
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
      expect(first.Key.pk).to.equal('2026-04-06');
      expect(first.Key.sk).to.equal('EXCLUDE~MILANO~RS');
      expect(first.ExpressionAttributeValues[':inc']).to.equal(1);

      expect(second.Key.sk).to.equal('EXCLUDE~ROMA~890');
      expect(second.ExpressionAttributeValues[':inc']).to.equal(2);
    });

    it('does not call Dynamo when all grouped records are filtered out', async () => {
      const excludeGroupedRecords = {
        'MILANO~RS': [{ communicationType: 'INFORMAL', kinesisSeqNumber: 'seq-rs' }],
        'ROMA~890': [{ attempt: '0', communicationType: 'LEGAL', kinesisSeqNumber: 'seq-non-rs' }]
      };

      const result = await dynamo.updateExcludeCounter(excludeGroupedRecords, []);

      expect(result).to.deep.equal([]);
      expect(mockSend.called).to.be.false;
    });

    it('adds only failed group sequence numbers when one update fails and continues others', async () => {
      const excludeGroupedRecords = {
        'MILANO~RS': [
          { communicationType: 'LEGAL', kinesisSeqNumber: 'seq-rs-1' },
          { communicationType: 'LEGAL', kinesisSeqNumber: 'seq-rs-2' }
        ],
        'ROMA~890': [
          { attempt: '1', communicationType: 'LEGAL', kinesisSeqNumber: 'seq-ok-1' }
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
        '@aws-sdk/client-dynamodb': { DynamoDBClient: function() {} },
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
        'MILANO~RS': [{ communicationType: 'REGISTERED_LETTER', kinesisSeqNumber: 'seq1' }]
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

