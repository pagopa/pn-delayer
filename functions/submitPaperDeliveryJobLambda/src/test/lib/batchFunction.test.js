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

  it('returns jobs in progress on first status', async () => {
    mockSend
      .onCall(0).resolves({ jobSummaryList: [{ jobId: '1' }] });

    const result = await listJobsByStatus();

    expect(result).to.deep.equal(true);
    sinon.assert.callCount(mockSend, 1);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(ListJobsCommand));

  });

  it('returns jobs in progress on second status', async () => {
    mockSend
      .onCall(0).resolves({ jobSummaryList: [] })
      .onCall(1).resolves({ jobSummaryList: [{ jobId: '1' }] });

    const result = await listJobsByStatus();

    expect(result).to.deep.equal(true);
    sinon.assert.callCount(mockSend, 2);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(ListJobsCommand));

  });

  it('returns an empty array when no jobs are in progress', async () => {
    mockSend.resolves({ jobSummaryList: [] });

    const result = await listJobsByStatus();

    expect(result).to.deep.equal(false);
    sinon.assert.callCount(mockSend, 5);
  });

  it('throws an error when the Batch client fails', async () => {
    mockSend.rejects(new Error('Batch error'));

    await expect(listJobsByStatus()).to.be.rejectedWith('Batch error');
    sinon.assert.callCount(mockSend, 1);
  });
});

describe('submitJobs', () => {
  const date = new Date().toISOString().slice(0, 16).replace(/\D/g, '');

  it('submits all jobs successfully', async () => {
    const tuples = "{\"driver1\": [\"province1\", \"province2\"], \"driver2\": [\"province3\"]}";
    mockSend.resolves({ jobId: '123' });

    await submitJobs(tuples, date);

    sinon.assert.callCount(mockSend, 2);
    sinon.assert.calledWith(mockSend, sinon.match.instanceOf(SubmitJobCommand));

  });

  it('submits no jobs', async () => {
    mockSend.resolves({ jobId: '123' });
    await submitJobs("{}", date);
    sinon.assert.notCalled(mockSend);
  });
});