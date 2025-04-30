const sinon = require('sinon');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const { expect } = chai;
const { handleEvent } = require('../app/eventHandler');
const batchFunction = require('../app/lib/batchFunction');
const ssmParameter = require('../app/lib/ssmParameter');
chai.use(chaiAsPromised);

let listJobsByStatusStub, retrieveUnifiedDeliveryDriverProvinceStub, submitJobsStub;

beforeEach(() => {
  listJobsByStatusStub = sinon.stub(batchFunction, 'listJobsByStatus');
  retrieveUnifiedDeliveryDriverProvinceStub = sinon.stub(ssmParameter, 'retrieveUnifiedDeliveryDriverProvince');
  submitJobsStub = sinon.stub(batchFunction, 'submitJobs');
});

afterEach(() => {
  sinon.restore();
});

describe('handleEvent', () => {
  it('returns an empty array when jobs are in progress', async () => {
    listJobsByStatusStub.resolves(true);

    const result = await handleEvent();

    expect(result).to.deep.equal([]);
    sinon.assert.callCount(listJobsByStatusStub, 1);
    sinon.assert.notCalled(retrieveUnifiedDeliveryDriverProvinceStub);
    sinon.assert.notCalled(submitJobsStub);
  });

  it('returns sent tuples when no jobs are in progress', async () => {
    listJobsByStatusStub.resolves(false);
    submitJobsStub.resolves(['job1#driver1~province1', 'job2#driver1~province2','job3#driver2~province3']);
    retrieveUnifiedDeliveryDriverProvinceStub.resolves({
    driver1: ['province1', 'province2'],
    driver2: ['province3']
  });

    const result = await handleEvent();

    expect(result).to.deep.equal(
      ['job1#driver1~province1',
      'job2#driver1~province2',
      'job3#driver2~province3']
  );
    sinon.assert.calledOnce(listJobsByStatusStub);
    sinon.assert.calledOnce(retrieveUnifiedDeliveryDriverProvinceStub);
    sinon.assert.calledWith(submitJobsStub, 
      ['driver1~province1',
      'driver1~province2',
      'driver2~province3']
    );
  });

  it('handles empty delivery driver province map gracefully', async () => {
    listJobsByStatusStub.resolves(false);
    retrieveUnifiedDeliveryDriverProvinceStub.resolves({});
  
    const result = await handleEvent();
  
    expect(result).to.deep.equal([]);
    sinon.assert.calledOnce(listJobsByStatusStub);
    sinon.assert.calledOnce(retrieveUnifiedDeliveryDriverProvinceStub);
    sinon.assert.notCalled(submitJobsStub);
  });

  it('throws an error when listJobsByStatus fails', async () => {
    listJobsByStatusStub.rejects(new Error('Batch error'));

    await expect(handleEvent()).to.be.rejectedWith('Batch error');
    sinon.assert.calledOnce(listJobsByStatusStub);
    sinon.assert.notCalled(retrieveUnifiedDeliveryDriverProvinceStub);
    sinon.assert.notCalled(submitJobsStub);
  });

  it('throws an error when retrieveUnifiedDeliveryDriverProvince fails', async () => {
    listJobsByStatusStub.resolves(false);
    retrieveUnifiedDeliveryDriverProvinceStub.rejects(new Error('SSM error'));

    await expect(handleEvent()).to.be.rejectedWith('SSM error');
    sinon.assert.calledOnce(listJobsByStatusStub);
    sinon.assert.calledOnce(retrieveUnifiedDeliveryDriverProvinceStub);
    sinon.assert.notCalled(submitJobsStub);
  });
});