const sinon = require('sinon');
const { expect } = require('chai');
const { getPriorityMap } = require('../../app/lib/ssmParameter');
const { SSMClient, GetParameterCommand } = require("@aws-sdk/client-ssm");

let mockSend;

beforeEach(() => {
  mockSend = sinon.stub();
  sinon.replace(SSMClient.prototype, 'send', mockSend);
});

afterEach(() => {
  sinon.restore();
});


describe('retrievePriorityMap', () => {
  process.env.PAPER_DELIVERY_PRIORITY_PARAMETER = "/config/pn-delayer/paper-delivery-priority";
  process.env.AWS_REGION = "us-east-1";

  it('returns the parameter value when retrieval is successful', async () => {
    const mockValue = '{"1":["PRODUCT_RS.ATTEMPT_0"],"2":["PRODUCT_AR.ATTEMPT_1","PRODUCT_890.ATTEMPT_1"],"3":["PRODUCT_AR.ATTEMPT_0","PRODUCT_890.ATTEMPT_0"]}';
    mockSend.resolves({ Parameter: { Value: mockValue } });
    const result = await getPriorityMap();

    expect(result).equal(mockValue);
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  });

  it('returns the empty map when retrieval is successful with empry object', async () => {
    const mockValue = {};
    mockSend.resolves({ Parameter: { Value: mockValue } });

    const result = await getPriorityMap();

    expect(result).equal(mockValue);
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  });

  it('returns the empty map when retrieval is successful with null value', async () => {
    const mockValue = null;
    mockSend.resolves({ Parameter: { Value: mockValue } });

    const result = await getPriorityMap();

    expect(result).to.deep.equal({});
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  });

  it('throws an error when the parameter does not exist', async () => {
    mockSend.rejects(new Error('ParameterNotFound'));

    await expect(getPriorityMap()).to.be.rejectedWith('ParameterNotFound');
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  
  });

  it('throws an error when the SSM client fails', async () => {
    mockSend.rejects(new Error('SSMClientError'));

    await expect(getPriorityMap()).to.be.rejectedWith('SSMClientError');
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  });
});
