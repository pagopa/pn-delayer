const sinon = require('sinon');
const { expect } = require('chai');
const { retrieveUnifiedDeliveryDriverProvince } = require('../../app/lib/ssmParameter');
const { SSMClient, GetParameterCommand } = require("@aws-sdk/client-ssm");

let mockSend;

beforeEach(() => {
  mockSend = sinon.stub();
  sinon.replace(SSMClient.prototype, 'send', mockSend);
});

afterEach(() => {
  sinon.restore();
});


describe('retrieveUnifiedDeliveryDriverProvince', () => {
  const parameterName = "JOB_INPUT_PARAMETER";
  process.env.JOB_INPUT_PARAMETER = parameterName;
  process.env.AWS_REGION = "us-east-1";

  it('returns the parameter value when retrieval is successful', async () => {
    const mockValue = '{"driver1":["province1","province2"],"driver2":["province3"]}';
    mockSend.resolves({ Parameter: { Value: mockValue } });

    const result = await retrieveUnifiedDeliveryDriverProvince();

    expect(result).equal(mockValue);
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  });

  it('returns the empty map when retrieval is successful with empry object', async () => {
    const mockValue = '{}';
    mockSend.resolves({ Parameter: { Value: mockValue } });

    const result = await retrieveUnifiedDeliveryDriverProvince();

    expect(result).equal(mockValue);
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  });

  it('returns the empty map when retrieval is successful with null value', async () => {
    const mockValue = null;
    mockSend.resolves({ Parameter: { Value: mockValue } });

    const result = await retrieveUnifiedDeliveryDriverProvince();

    expect(result).to.deep.equal({});
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  });

  it('throws an error when the parameter does not exist', async () => {
    mockSend.rejects(new Error('ParameterNotFound'));

    await expect(retrieveUnifiedDeliveryDriverProvince()).to.be.rejectedWith('ParameterNotFound');
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  
  });

  it('throws an error when the SSM client fails', async () => {
    mockSend.rejects(new Error('SSMClientError'));

    await expect(retrieveUnifiedDeliveryDriverProvince()).to.be.rejectedWith('SSMClientError');
    sinon.assert.calledOnce(mockSend);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(GetParameterCommand));
  });
});
