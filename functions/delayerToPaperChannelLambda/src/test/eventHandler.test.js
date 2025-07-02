const sinon = require('sinon');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const { handleEvent } = require('../app/eventHandler');
const dynamo = require('../app/lib/dynamo');
const sqsSender = require('../app/lib/sqsSender');
const parameterStore = require('../app/lib/ssmParameter');
chai.use(chaiAsPromised);
const { expect } = chai;


let deleteItemsStub, prepareAndSendSqsMessagesStub;

beforeEach(() => {
    deleteItemsStub = sinon.stub(dynamo, 'deleteItems');
    prepareAndSendSqsMessagesStub = sinon.stub(sqsSender, 'prepareAndSendSqsMessages');
    process.env.PAPER_DELIVERY_READYTOSEND_QUERYLIMIT = '1000';
});

afterEach(() => {
    sinon.restore();
});

describe('handleEvent', () => {

    it('should not process items if daily and weekly capacities are exhausted', async () => {
      const priorityMap = { 1: 'RS', 2: '2TENT', 3: 'AR/890' };
      sinon.stub(parameterStore, 'getPriorityMap').resolves(priorityMap);
      sinon.stub(dynamo, 'getUsedPrintCapacities').resolves({ daily: {capacity: 10, usedCapacity: 10}, weekly: {usedCapacity: 70} });
      sinon.stub(dynamo, 'getPrintCapacity').resolves(10);
      process.env.PN_DELAYER_WEEKLY_WORKING_DAYS = '7';
      const dynamoDbGetItem = sinon.stub(dynamo, 'getItems');
      const consoleErrorStub = sinon.stub(console, 'error');
      const updatePrintCapacityCounterStub = sinon.stub(dynamo, 'updatePrintCapacityCounter')

      await handleEvent();

      sinon.assert.notCalled(updatePrintCapacityCounterStub);
      sinon.assert.notCalled(dynamoDbGetItem);
      sinon.assert.calledWith(consoleErrorStub, sinon.match(/Both daily and weekly print capacities exhausted/));
      consoleErrorStub.restore();
    });

    it('should log and throw if weekly exhausted but daily not', async () => {
       const priorityMap = { 1: 'RS', 2: '2TENT', 3: 'AR/890' };
      sinon.stub(parameterStore, 'getPriorityMap').resolves(priorityMap);
      sinon.stub(dynamo, 'getUsedPrintCapacities').resolves({ daily: {capacity: 10, usedCapacity:5}, weekly: { usedCapacity: 70 }});
      process.env.PN_DELAYER_WEEKLY_WORKING_DAYS = '7';
      const dynamoDbGetItem = sinon.stub(dynamo, 'getItems');
      const consoleErrorStub = sinon.stub(console, 'error');
      const updatePrintCapacityCounterStub = sinon.stub(dynamo, 'updatePrintCapacityCounter')

      await expect(handleEvent()).to.be.rejectedWith('Weekly print capacity exhausted but daily capacity not exhausted. This should not happen.');
      
      sinon.assert.notCalled(updatePrintCapacityCounterStub);
      sinon.assert.notCalled(dynamoDbGetItem);
      sinon.assert.calledWith(consoleErrorStub, sinon.match(/Weekly print capacity exhausted but daily capacity not exhausted/));
      consoleErrorStub.restore();
    });

    it('should log and exit if daily exhausted but weekly not', async () => {
      const priorityMap = { 1: 'RS', 2: '2TENT', 3: 'AR/890' };
      sinon.stub(parameterStore, 'getPriorityMap').resolves(priorityMap);
      sinon.stub(dynamo, 'getUsedPrintCapacities').resolves({ daily: {capacity: 10, usedCapacity:10}, weekly: { usedCapacity: 10 }});
      process.env.PN_DELAYER_WEEKLY_WORKING_DAYS = '7';
      const dynamoDbGetItem = sinon.stub(dynamo, 'getItems');
      const consoleStub = sinon.stub(console, 'log');
      const updatePrintCapacityCounterStub = sinon.stub(dynamo, 'updatePrintCapacityCounter')

      await handleEvent();

      sinon.assert.notCalled(dynamoDbGetItem);
      sinon.assert.notCalled(updatePrintCapacityCounterStub);
      sinon.assert.calledWith(consoleStub, sinon.match(/No items processed for priority 1 and executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleStub, sinon.match(/No items processed for priority 2 and executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleStub, sinon.match(/No items processed for priority 3 and executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleStub, sinon.match(/Daily print capacity exhausted/));
      consoleStub.restore();
    });

    it('should update print capacity counters when one item for priorityKey are processed', async () => {
      const priorityMap = { 1: 'RS', 2: '2TENT', 3: 'AR/890' };
      sinon.stub(parameterStore, 'getPriorityMap').resolves(priorityMap);
      sinon.stub(dynamo, 'getUsedPrintCapacities').resolves({ daily: null, weekly: null });
      sinon.stub(dynamo, 'getPrintCapacity').resolves(10);
      sinon.stub(dynamo, 'getItems').resolves({ Items: [{ id: '1' }], LastEvaluatedKey: null });
      process.env.PN_DELAYER_WEEKLY_WORKING_DAYS = '7';
      prepareAndSendSqsMessagesStub.resolves({ successes: ['1'], failures: [] });
      deleteItemsStub.resolves([]);

      const updatePrintCapacityCounterStub = sinon.stub(dynamo, 'updatePrintCapacityCounter').resolves('OK');
      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.callCount(updatePrintCapacityCounterStub, 2);      
      sinon.assert.callCount(dynamo.getItems, 3);
      expect(dynamo.getItems.getCall(0).args[0]).to.equal('1');
      expect(dynamo.getItems.getCall(1).args[0]).to.equal('2');
      expect(dynamo.getItems.getCall(2).args[0]).to.equal('3');
      expect(dynamo.getItems.getCall(0).args[3]).to.equal(10);
      expect(dynamo.getItems.getCall(1).args[3]).to.equal(9);
      expect(dynamo.getItems.getCall(2).args[3]).to.equal(8);
      sinon.assert.callCount(dynamo.getItems, 3);
      sinon.assert.calledWith(updatePrintCapacityCounterStub, sinon.match.any, 'DAY', 3);
      sinon.assert.calledWith(updatePrintCapacityCounterStub, sinon.match.any, 'WEEK', 3);
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 1 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 1 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Processed 1 of priority 1 items for executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 1 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 1 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Processed 1 of priority 2 items for executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 1 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 1 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Processed 1 of priority 3 items for executionDate: 2025-07-02/));
      consoleLogStub.restore();
    });

    it('should update print capacity counters when one item for priorityKey are processed but with error on sqs', async () => {
          const priorityMap = { 1: 'RS', 2: '2TENT', 3: 'AR/890' };
          sinon.stub(parameterStore, 'getPriorityMap').resolves(priorityMap);
          sinon.stub(dynamo, 'getUsedPrintCapacities').resolves({ daily: null, weekly: null });
          sinon.stub(dynamo, 'getPrintCapacity').resolves(10);
          sinon.stub(dynamo, 'getItems').resolves({ Items: [{ id: '1' }], LastEvaluatedKey: null });
          process.env.PN_DELAYER_WEEKLY_WORKING_DAYS = '7';
          prepareAndSendSqsMessagesStub
            .onFirstCall().resolves({ successes: [], failures: ['1'] })
            .onSecondCall().resolves({ successes: ['1'], failures: [] })
            .onThirdCall().resolves({ successes: [], failures: ['1'] });
          deleteItemsStub.resolves([]);

          const updatePrintCapacityCounterStub = sinon.stub(dynamo, 'updatePrintCapacityCounter').resolves('OK');
          const consoleLogStub = sinon.stub(console, 'log');

          await handleEvent();
          // New assertions: dynamo.getItems called 3 times with correct priorityKey and queryLimit
          sinon.assert.callCount(dynamo.getItems, 3);
          expect(dynamo.getItems.getCall(0).args[0]).to.equal('1');
          expect(dynamo.getItems.getCall(1).args[0]).to.equal('2');
          expect(dynamo.getItems.getCall(2).args[0]).to.equal('3');
          expect(dynamo.getItems.getCall(0).args[3]).to.equal(10);
          expect(dynamo.getItems.getCall(1).args[3]).to.equal(10);
          expect(dynamo.getItems.getCall(2).args[3]).to.equal(9);
          sinon.assert.callCount(dynamo.getItems, 3);
          sinon.assert.callCount(updatePrintCapacityCounterStub, 2);
          sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 0 messages/));
          sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 1 messages/));
          sinon.assert.calledWith(consoleLogStub, sinon.match(/No items to delete as no messages were successfully sent./));
          sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 1 messages/));
          sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
          sinon.assert.calledWith(consoleLogStub, sinon.match(/No items to delete as no messages were successfully sent./));
          sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 0 messages/));
          sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 1 messages/));
          sinon.assert.calledWith(consoleLogStub, sinon.match(/No items to delete as no messages were successfully sent./));
          consoleLogStub.restore();
        });

    it('should update print capacity counters when no more printCapacity for first key', async () => {
      const priorityMap = { 1: 'RS', 2: '2TENT', 3: 'AR/890' };
      sinon.stub(parameterStore, 'getPriorityMap').resolves(priorityMap);
      sinon.stub(dynamo, 'getUsedPrintCapacities').resolves({ daily: null, weekly: null });
      sinon.stub(dynamo, 'getPrintCapacity').resolves(10);
      sinon.stub(dynamo, 'getItems')
        .onFirstCall().resolves({ Items: [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }, { id: '5' }], LastEvaluatedKey: { id: '5' } })
        .onSecondCall().resolves({ Items: [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }, { id: '5' }], LastEvaluatedKey: {} });
      process.env.PN_DELAYER_WEEKLY_WORKING_DAYS = '7';
      prepareAndSendSqsMessagesStub.resolves({ successes: ['1','1','1','1','1'], failures: [] });
      deleteItemsStub.resolves([]);

      const updatePrintCapacityCounterStub = sinon.stub(dynamo, 'updatePrintCapacityCounter').resolves('OK');
      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.callCount(updatePrintCapacityCounterStub, 2);
      expect(dynamo.getItems.getCall(0).args[0]).to.equal('1');
      expect(dynamo.getItems.getCall(1).args[0]).to.equal('1');
      expect(dynamo.getItems.getCall(0).args[3]).to.equal(10);
      expect(dynamo.getItems.getCall(1).args[3]).to.equal(5);
      sinon.assert.callCount(dynamo.getItems, 2);
      sinon.assert.calledWith(updatePrintCapacityCounterStub, sinon.match.any, 'DAY', 10);
      sinon.assert.calledWith(updatePrintCapacityCounterStub, sinon.match.any, 'WEEK', 10);
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 5 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 5 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 5 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 5 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Remaining Daily Capacity: 0 - Remaining Weekly Capacity: 60/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Remaining Daily Capacity: 0 - Remaining Weekly Capacity: 60/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Remaining Daily Capacity: 0 - Remaining Weekly Capacity: 60/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/used daily capacity incremented of: 10 - used weekly capacity incremented of: 10/));
      consoleLogStub.restore();
    });

    it('should update print capacity counters when no more printCapacity for second key', async () => {
      const priorityMap = { 1: 'RS', 2: '2TENT', 3: 'AR/890' };
      sinon.stub(parameterStore, 'getPriorityMap').resolves(priorityMap);
      sinon.stub(dynamo, 'getUsedPrintCapacities').resolves({ daily: null, weekly: null });
      sinon.stub(dynamo, 'getPrintCapacity').resolves(10);
      sinon.stub(dynamo, 'getItems')
        .onFirstCall().resolves({ Items: [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }, { id: '5' }], LastEvaluatedKey: {} })
        .onSecondCall().resolves({ Items: [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }, { id: '5' }], LastEvaluatedKey: {} });
      process.env.PN_DELAYER_WEEKLY_WORKING_DAYS = '7';
      prepareAndSendSqsMessagesStub.resolves({ successes: ['1','1','1','1','1'], failures: [] });
      deleteItemsStub.resolves([]);

      const updatePrintCapacityCounterStub = sinon.stub(dynamo, 'updatePrintCapacityCounter').resolves('OK');
      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.callCount(updatePrintCapacityCounterStub, 2);
      expect(dynamo.getItems.getCall(0).args[0]).to.equal('1');
      expect(dynamo.getItems.getCall(1).args[0]).to.equal('2');
      expect(dynamo.getItems.getCall(0).args[3]).to.equal(10);
      expect(dynamo.getItems.getCall(1).args[3]).to.equal(5);
      sinon.assert.callCount(dynamo.getItems, 2);
      sinon.assert.calledWith(updatePrintCapacityCounterStub, sinon.match.any, 'DAY', 10);
      sinon.assert.calledWith(updatePrintCapacityCounterStub, sinon.match.any, 'WEEK', 10);
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 5 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 5 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Processed 5 of priority 1 items for executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 5 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 5 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Remaining Daily Capacity: 0 - Remaining Weekly Capacity: 60/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Remaining Daily Capacity: 0 - Remaining Weekly Capacity: 60/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/used daily capacity incremented of: 10 - used weekly capacity incremented of: 10/));
      consoleLogStub.restore();
    });

    it('should update print capacity counters when no more printCapacity for third key', async () => {
      const priorityMap = { 1: 'RS', 2: '2TENT', 3: 'AR/890' };
      sinon.stub(parameterStore, 'getPriorityMap').resolves(priorityMap);
      sinon.stub(dynamo, 'getUsedPrintCapacities').resolves({ daily: null, weekly: null });
      sinon.stub(dynamo, 'getPrintCapacity').resolves(10);
      sinon.stub(dynamo, 'getItems')
        .onFirstCall().resolves({ Items: [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }], LastEvaluatedKey: {} })
        .onSecondCall().resolves({ Items: [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }], LastEvaluatedKey: {} })
        .onThirdCall().resolves({ Items: [{ id: '1' }, { id: '2' }], LastEvaluatedKey: {} });
      process.env.PN_DELAYER_WEEKLY_WORKING_DAYS = '7';
      prepareAndSendSqsMessagesStub
        .onFirstCall().resolves({ successes: ['1','1','1','1'], failures: [] })
        .onSecondCall().resolves({ successes:['1','1','1','1'], failures: [] })
        .onThirdCall().resolves({ successes: ['1','1'], failures: [] });
      deleteItemsStub.resolves([]);

      const updatePrintCapacityCounterStub = sinon.stub(dynamo, 'updatePrintCapacityCounter').resolves('OK');
      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.callCount(updatePrintCapacityCounterStub, 2);
      expect(dynamo.getItems.getCall(0).args[0]).to.equal('1');
      expect(dynamo.getItems.getCall(1).args[0]).to.equal('2');
      expect(dynamo.getItems.getCall(2).args[0]).to.equal('3');
      expect(dynamo.getItems.getCall(0).args[3]).to.equal(10);
      expect(dynamo.getItems.getCall(1).args[3]).to.equal(6);
      expect(dynamo.getItems.getCall(2).args[3]).to.equal(2);
      sinon.assert.callCount(dynamo.getItems, 3);
      sinon.assert.calledWith(updatePrintCapacityCounterStub, sinon.match.any, 'DAY', 10);
      sinon.assert.calledWith(updatePrintCapacityCounterStub, sinon.match.any, 'WEEK', 10);
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 4 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 4 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Processed 4 of priority 1 items for executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 4 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 4 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Processed 4 of priority 2 items for executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 2 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 2 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Processed 2 of priority 3 items for executionDate: 2025-07-02/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Remaining Daily Capacity: 0 - Remaining Weekly Capacity: 60/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/used daily capacity incremented of: 10 - used weekly capacity incremented of: 10/));
      consoleLogStub.restore();
    });
});