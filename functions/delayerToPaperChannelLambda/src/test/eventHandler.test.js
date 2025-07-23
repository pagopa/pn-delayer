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
        input: {
          dailyPrintCapacity: 10,
          weeklyPrintCapacity: 70,
          numberOfShipments: 100,
          lastEvaluatedKeyPhase2: {},
          sendToNextStepCounter: 0,
          executionDate: '2025-07-01T00:00:00Z'
        }
      });
    } catch (err) {
      expect(err.message).to.include('Invalid processType. Use: SEND_TO_PHASE_2 or SEND_TO_NEXT_WEEK');
    }
  });

  it('should execute SEND_TO_PHASE_2 and process items', async () => {
    retrieveStub
      .onCall(0).resolves({
        Items: [{ id: 1 }],
        LastEvaluatedKey: null
      });

    const result = await handler.handleEvent({
      processType: 'SEND_TO_PHASE_2',
      input: {
        dailyPrintCapacity: 10,
        weeklyPrintCapacity: 70,
        numberOfShipments: 100,
        lastEvaluatedKeyPhase2: {},
        sendToNextStepCounter: 0,
        executionDate: '2025-07-01T00:00:00Z'
      }
    });

    expect(retrieveStub.calledOnce).to.be.true;
    expect(mapStub.calledOnce).to.be.true;
    expect(insertStub.calledOnce).to.be.true;
    expect(result.input).to.deep.include({
      sendToNextStepCounter: 1,
      lastEvaluatedKeyPhase2: null,
      dailyPrintCapacity: 10,
      weeklyPrintCapacity: 70,
      numberOfShipments: 100,
      executionDate: '2025-07-01T00:00:00Z'
    });
  });

  it('should execute SEND_TO_NEXT_WEEK and process items recursively no items to process', async () => {
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
      input: {
        dailyPrintCapacity: 4,
        weeklyPrintCapacity: 28,
        numberOfShipments: 20,
        lastEvaluatedKeyNextWeek: {},
        sendToNextWeekCounter: 0,
        sentToNextWeek: 0,
        executionDate: '2025-07-01T00:00:00Z'
      },
    });

    expect(retrieveStub.callCount).to.equal(0);
    expect(mapStub.callCount).to.equal(0);
    expect(insertStub.callCount).to.equal(0);
    expect(result.input).to.deep.include({
      sendToNextWeekCounter: 0,
      lastEvaluatedKeyNextWeek: null,
      dailyPrintCapacity: 4,
      weeklyPrintCapacity: 28,
      numberOfShipments: 20,
      executionDate: '2025-07-01T00:00:00Z',
      sentToNextWeek: 0
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
      input: {
        dailyPrintCapacity: 4,
        weeklyPrintCapacity: 28,
        numberOfShipments: 8000,
        lastEvaluatedKeyNextWeek: {},
        sendToNextWeekCounter: 0,
        sentToNextWeek: 0,
        executionDate: '2025-07-01T00:00:00Z'
      },
    });

    expect(retrieveStub.callCount).to.equal(2);
    expect(mapStub.callCount).to.equal(3);
    expect(insertStub.callCount).to.equal(2);
    expect(result.input).to.deep.include({
      sendToNextWeekCounter: 3,
      lastEvaluatedKeyNextWeek: null,
      dailyPrintCapacity: 4,
      weeklyPrintCapacity: 28,
      numberOfShipments: 8000,
      executionDate: '2025-07-01T00:00:00Z',
      sentToNextWeek: 0
    });
  });
});
