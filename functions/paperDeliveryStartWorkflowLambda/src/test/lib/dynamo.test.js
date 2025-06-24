const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');

describe('retrieveProvinceWithPaperDeliveries', () => {
  let mockSend;
  let QueryCommand;
  let dynamo;

  beforeEach(() => {
    mockSend = sinon.stub();

    QueryCommand = function(params) { this.params = params; };

    const DynamoDBDocumentClient = {
      from: sinon.stub().returns({ send: mockSend })
    };

    dynamo = proxyquire('../../app/lib/dynamo', {
      '@aws-sdk/client-dynamodb': { DynamoDBClient: function() {} },
      '@aws-sdk/lib-dynamodb': {
        QueryCommand,
        DynamoDBDocumentClient
      }
    });

    process.env.PAPER_DELIVERY_COUNTER_TABLENAME = 'testCounterTable';
  });

  afterEach(() => {
    sinon.restore();
    delete process.env.PAPER_DELIVERY_COUNTER_TABLENAME;
  });

  it('return item founded on CounterTable', async () => {
    const fakeItems = [{ sk: 'EVAL~MI' }, { sk: 'EVAL~RM' }];
    mockSend.resolves({ Items: fakeItems });
    const result = await dynamo.retrieveProvinceWithPaperDeliveries('2024-06-10');
    expect(result).to.deep.equal(fakeItems);
    expect(mockSend.calledOnce).to.be.true;
  });

  it('return undefined if response does not contain items', async () => {
    mockSend.resolves({ Items: [] });

    const result = await dynamo.retrieveProvinceWithPaperDeliveries('2024-06-10');
    expect(result).to.deep.equal([]);
  });

  it('return error if the query does not succeed', async () => {
    mockSend.rejects(new Error('DynamoDB error'));

    try {
      await dynamo.retrieveProvinceWithPaperDeliveries('2024-06-10');
      expect.fail('it should have thrown an error');
    } catch (err) {
      expect(err.message).to.equal('DynamoDB error');
    }
  });
});