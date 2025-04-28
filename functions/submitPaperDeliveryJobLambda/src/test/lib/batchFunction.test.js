const sinon = require('sinon');
const { expect } = require('chai');
const { listJobsByStatus, submitJobs } = require('../../app/lib/batchFunction');
const { BatchClient, ListJobsCommand, SubmitJobCommand } = require("@aws-sdk/client-batch");

let mockSend;

beforeEach(() => {
  mockSend = sinon.stub();
  sinon.replace(BatchClient.prototype, 'send', mockSend);
});

afterEach(() => {
  sinon.restore();
});

describe('listJobsByStatus', () => {

  it('returns all jobs in progress across multiple statuses', async () => {
    mockSend
      .onCall(0).resolves({ jobSummaryList: [{ jobId: '1' }] })
      .onCall(1).resolves({ jobSummaryList: [{ jobId: '2' }] })
      .onCall(2).resolves({ jobSummaryList: [] })
      .onCall(3).resolves({ jobSummaryList: [] })
      .onCall(4).resolves({ jobSummaryList: [{ jobId: '3' }] });

    const result = await listJobsByStatus();

    expect(result).to.deep.equal([{ jobId: '1' }, { jobId: '2' }, { jobId: '3' }]);
    sinon.assert.callCount(mockSend, 5);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(ListJobsCommand));

  });

  it('returns an empty array when no jobs are in progress', async () => {
    mockSend.resolves({ jobSummaryList: [] });

    const result = await listJobsByStatus();

    expect(result).to.deep.equal([]);
    sinon.assert.callCount(mockSend, 5);
  });

  it('throws an error when the Batch client fails', async () => {
    mockSend.rejects(new Error('Batch error'));

    await expect(listJobsByStatus()).to.be.rejectedWith('Batch error');
    sinon.assert.callCount(mockSend, 1);
  });
});

describe('submitJobs', () => {

  const tuples = ['driver1~province1', 'driver2~province2'];

  it('submits all jobs successfully', async () => {
    mockSend.resolves({ jobId: '123' });

    await submitJobs(tuples);

    sinon.assert.callCount(mockSend, 2);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(SubmitJobCommand));

  });

  it('throws an error when job submission fails', async () => {
    mockSend.rejects(new Error('Submit error'));

    await expect(submitJobs(tuples)).to.be.rejectedWith('Submit error');
    sinon.assert.callCount(mockSend, 1);
  });
});