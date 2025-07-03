  const { expect } = require('chai');
  const sinon = require('sinon');
  const proxyquire = require('proxyquire');

  describe('dynamo.js', () => {
    let dynamo;
    let mockSend;
    let BatchWriteCommand, BatchGetCommand;

    beforeEach(() => {
      mockSend = sinon.stub();

      BatchWriteCommand = function(params) { this.params = params; };
      BatchGetCommand = function(params) { this.params = params; };

      const DynamoDBDocumentClient = {
        from: sinon.stub().returns({ send: mockSend })
      };

      dynamo = proxyquire('../app/lib/dynamo', {
        '@aws-sdk/client-dynamodb': { DynamoDBClient: function() {} },
        '@aws-sdk/lib-dynamodb': {
          BatchWriteCommand,
          BatchGetCommand,
          DynamoDBDocumentClient
        }
      });

      process.env.PAPER_DELIVERY_INCOMING_TABLE_NAME = 'TestIncomingTable';
      process.env.KINESIS_PAPER_DELIVERY_EVENT_TABLE_NAME = "KinesisPaperDeliveryEventTable";
      process.env.PAPER_DELIVERY_COUNTER_TABLE_NAME = 'TestCounterTable';
      process.env.BATCH_SIZE = '25';
    });

    describe('batchWriteIncomingRecords', () => {
      it('handle successful batch write', async () => {
        const records = [
          { entity: { requestId: '1' }, kinesisSeqNumber: 'seq1' }
        ];
        mockSend.resolves({ UnprocessedItems: {} });

        const result = await dynamo.batchWriteIncomingRecords(records, []);

        expect(mockSend.calledOnce).to.be.true;
        expect(result).to.deep.equal([]);
        expect(mockSend.firstCall.args[0]).to.be.instanceOf(BatchWriteCommand);
      });

      it('handle UnprocessedItems', async () => {
        const records = [
          { entity: { requestId: '1' }, kinesisSeqNumber: 'seq1' }
        ];
        mockSend.resolves({
          UnprocessedItems: {
            TestIncomingTable: [
              { PutRequest: { Item: { requestId: { S: '1' } } } }
            ]
          }
        });

        const result = await dynamo.batchWriteIncomingRecords(records, []);

        expect(result).to.deep.equal([{ itemIdentifier: 'seq1' }]);
      });

      it('handle errors on batch write', async () => {
        const records = [
          { entity: { requestId: '1' }, kinesisSeqNumber: 'seq1' }
        ];
        mockSend.rejects(new Error('fail'));

        const result = await dynamo.batchWriteIncomingRecords(records, []);

        expect(result).to.deep.equal([{ itemIdentifier: 'seq1' }]);
      });

      it('handle empty array', async () => {
        const records = [];
        mockSend.resolves({ UnprocessedItems: {} });

        const result = await dynamo.batchWriteIncomingRecords(records, []);

        expect(result).to.deep.equal([]);
      });
    });

    describe('batchWriteKinesisEventRecords', () => {
      it('write records on PaperDeliveryKinesisEvents', async () => {
        const records = [
          { requestId: 'seq1' }
        ];
        mockSend.resolves({ UnprocessedItems: {} });

        const result = await dynamo.batchWriteKinesisEventRecords(records);

        expect(mockSend.calledOnce).to.be.true;
        expect(result).to.deep.equal({ UnprocessedItems: {} });
        expect(mockSend.firstCall.args[0]).to.be.instanceOf(BatchWriteCommand);
      });

      it('handle DynamoDB errors', async () => {
        const records = [{ requestId: 'seq1' }];
        mockSend.rejects(new Error('DynamoDB error'));

        try {
          await dynamo.batchWriteKinesisEventRecords(records);
          expect.fail('Doveva lanciare');
        } catch (err) {
          expect(err.message).to.equal('DynamoDB error');
        }
      });
    });

    describe('batchGetKinesisEventRecords', () => {
      it('return requestId if present', async () => {
        const keys = ['seq1', 'seq2'];
        mockSend.resolves({
          Responses: {
            KinesisPaperDeliveryEventTable: [
              { requestId: 'seq1' },
              { requestId: 'seq2' }
            ]
          }
        });

        const result = await dynamo.batchGetKinesisEventRecords(keys);

        expect(result).to.deep.equal(['seq1', 'seq2']);
        expect(mockSend.firstCall.args[0]).to.be.instanceOf(BatchGetCommand);
      });

      it('return an empty array if no item is present', async () => {
        const keys = ['seq1'];
        mockSend.resolves({
          Responses: { TestKinesisTable: [] }
        });

        const result = await dynamo.batchGetKinesisEventRecords(keys);

        expect(result).to.deep.equal([]);
      });

      it('handle DynamoDB error', async () => {
        const keys = ['seq1'];
        mockSend.rejects(new Error('DynamoDB get error'));

        try {
          await dynamo.batchGetKinesisEventRecords(keys);
          expect.fail('Doveva lanciare');
        } catch (err) {
          expect(err.message).to.equal('DynamoDB get error');
        }
      });

      it('handle empty keys array', async () => {
        const keys = [];
        mockSend.resolves({
          Responses: { TestKinesisTable: [] }
        });

        const result = await dynamo.batchGetKinesisEventRecords(keys);

        expect(result).to.deep.equal([]);
      });
    });
  });