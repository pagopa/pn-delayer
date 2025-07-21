const { expect } = require('chai');
const sinon = require('sinon');
const { LocalDate } = require('@js-joda/core');

const handler = require("../app/eventHandler.js"); // aggiorna il path se necessario
const dynamo = require('../app/lib/dynamo');
const utils = require('../app/lib/utils');

describe('handleEvent', () => {
  let retrieveStub;
  let mapStub;
  let insertStub;

  beforeEach(() => {
    retrieveStub = sinon.stub(dynamo, 'retrieveItems');
    mapStub = sinon.stub(utils, 'mapToPaperDeliveryForGivenStep');
    insertStub = sinon.stub(dynamo, 'insertItems');

    process.env.PN_DELAYER_QUERY_LIMIT = '1000';
    process.env.PN_DELAYER_DELIVERYDATEDAYOFWEEK = '1';
  });

  afterEach(() => {
    sinon.restore();
  });

  it('should throw error when processType is missing', async () => {
    try {
      await handler.handleEvent({});
    } catch (err) {
      expect(err.message).to.equal('processType is required in the event');
    }
  });

  it('should throw error when processType is invalid', async () => {
    try {
      await handler.handleEvent({
        processType: 'UNKNOWN',
        executionDate: LocalDate.parse('2025-07-20')
      });
    } catch (err) {
      expect(err.message).to.include('Invalid processType');
    }
  });

  it('should execute SEND_TO_PHASE_2 and process items', async () => {
    retrieveStub
      .onFirstCall().resolves({
        Items: [{ id: 1 }],
        LastEvaluatedKey: null
      });

    const result = await handler.handleEvent({
      processType: 'SEND_TO_PHASE_2',
      lastEvaluatedKeyPhase2: null,
      toNextStepIncrementCounter: 0,
      executionDate: LocalDate.parse('2025-07-21')
    });

    expect(retrieveStub.called).to.be.true;
    expect(mapStub.calledOnce).to.be.true;
    expect(insertStub.calledOnce).to.be.true;
    expect(result).to.deep.include({
      toNextStepIncrementCounter: 1,
      lastEvaluatedKey: null
    });
  });

  it('should execute SEND_TO_NEXT_WEEK and process items recursively', async () => {
    retrieveStub
      .onCall(0).resolves({
        Items: [{ id: 1 }, { id: 2 }],
        LastEvaluatedKey: { id: 'last' }
      })
      .onCall(1).resolves({
        Items: [{ id: 3 }],
        LastEvaluatedKey: null
      });

    const result = await handler.handleEvent({
      processType: 'SEND_TO_NEXT_WEEK',
      lastEvaluatedKeyNextWeek: null,
      sendToNextWeekCounter: 10,
      toNextWeekIncrementCounter: 0,
      executionDate: LocalDate.parse('2025-07-21')
    });

    expect(retrieveStub.callCount).to.equal(2);
    expect(mapStub.callCount).to.equal(3);
    expect(insertStub.callCount).to.equal(2);
    expect(result).to.deep.include({
      toNextWeekIncrementCounter: 3,
      lastEvaluatedKey: null,
      sendToNextWeekCounter: 7
    });
  });
});
