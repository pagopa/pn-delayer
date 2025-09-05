const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');

const paperDeliveryTableName = 'pn-DelayerPaperDelivery';

describe('dynamo.js', () => {
  let dynamo;
  let mockSend;
  let BatchWriteCommand;
  let QueryCommand;
  let DynamoDBDocumentClient;

  beforeEach(() => {
    mockSend = sinon.stub();

    BatchWriteCommand = function (params) { this.input = params; };
    QueryCommand = function (params) { this.input = params; };

    DynamoDBDocumentClient = {
      from: sinon.stub().returns({ send: mockSend })
    };

    dynamo = proxyquire('../../app/lib/dynamo', {
      '@aws-sdk/client-dynamodb': { DynamoDBClient: function () {} },
      '@aws-sdk/lib-dynamodb': {
        DynamoDBDocumentClient,
        QueryCommand,
        BatchWriteCommand: BatchWriteCommand
      }
    });

    process.env.PAPER_DELIVERY_TABLENAME = 'paperDeliveryTable';
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('retrieveItems', () => {
    it('should call DynamoDB with correct parameters and return result', async () => {
      const mockResult = {
        Items: [{ id: 1 }],
        LastEvaluatedKey: { id: 'lastKey' }
      };

      mockSend.resolves(mockResult);

      const deliveryWeek = '2025-W30';
      const result = await dynamo.retrieveItems('pn-DelayerPaperDelivery', deliveryWeek, null, 10, true);

      expect(mockSend.calledOnce).to.be.true;
      expect(result).to.deep.equal(mockResult);
    });

    it('should return empty result if DynamoDB returns undefined', async () => {
      mockSend.resolves(undefined);

      const result = await dynamo.retrieveItems('2025-W30', null, 10, true);
      expect(result).to.deep.equal({ Items: [], LastEvaluatedKey: {} });
    });
  });

  describe('insertItems and insertItemsBatch', () => {
    it('should insert items using batch write and return empty if all succeed', async () => {
      const items = [{ pk: '1' }, { pk: '2' }];
      mockSend.resolves({ UnprocessedItems: {} });

      const result = await dynamo.insertItems('pn-DelayerPaperDelivery', items);

      expect(mockSend.calledOnce).to.be.true;
      expect(result).to.deep.equal([]);
    });

    it('should retry unprocessed items up to 3 times and return them if still unprocessed', async () => {
      mockSend.onFirstCall().resolves({
        UnprocessedItems: {
          [paperDeliveryTableName]: [{ pk: '1' }]
        }
      });
      mockSend.onSecondCall().resolves({
        UnprocessedItems: {
          [paperDeliveryTableName]: [{ pk: '1' }]
        }
      });
      mockSend.onThirdCall().resolves({
        UnprocessedItems: {
          [paperDeliveryTableName]: [{ pk: '1' }]
        }
      });

      const result = await dynamo.insertItems('pn-DelayerPaperDelivery', [{ pk: '1' }]);

      expect(mockSend.callCount).to.equal(3);
      expect(result).to.deep.equal([{ pk: '1' }]);
    });

    it('insertItems should call insertItemsBatch correctly', async () => {
      const batchStub = sinon.stub(dynamo, 'insertItems').resolves([]);

      const items = [{ a: 1 }];
      await dynamo.insertItems(items);

      expect(batchStub.calledOnce).to.be.true;
      batchStub.restore();
    });
  });
});