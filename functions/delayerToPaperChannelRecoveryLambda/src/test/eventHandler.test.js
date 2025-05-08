const sinon = require('sinon');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const { handleEvent } = require('../app/eventHandler');
const dynamo = require('../app/lib/dynamo');
const sqsSender = require('../app/lib/sqsSender');
chai.use(chaiAsPromised);
const { expect } = chai;

let getItemsChunkStub, deleteItemsStub, prepareAndSendSqsMessagesStub;

beforeEach(() => {
    getItemsChunkStub = sinon.stub(dynamo, 'getItemsChunk');
    deleteItemsStub = sinon.stub(dynamo, 'deleteItems');
    prepareAndSendSqsMessagesStub = sinon.stub(sqsSender, 'prepareAndSendSqsMessages');
});

afterEach(() => {
    sinon.restore();
});

 describe('handleEvent', () => {
    it('logs and exits when no items are found', async () => {
      getItemsChunkStub.resolves({ items: [], lastKey: null });
      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.calledOnce(getItemsChunkStub);
      sinon.assert.calledWith(consoleLogStub, sinon.match(/No items found for deliveryDate/));
      sinon.assert.notCalled(prepareAndSendSqsMessagesStub);
      sinon.assert.notCalled(deleteItemsStub);

      consoleLogStub.restore();
    });

    it('sends messages and deletes successfully processed items', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
      const sendMessageResponse = {
        successes: ['1'],
        failures: ['2']
      };
      const unprocessedItems = [];

      getItemsChunkStub.resolves({ items, lastKey: null });
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);
      deleteItemsStub.resolves(unprocessedItems);

      const consoleLogStub = sinon.stub(console, 'log');
      const consoleErrorStub = sinon.stub(console, 'error');

      await handleEvent();

      sinon.assert.calledOnce(getItemsChunkStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledWith(prepareAndSendSqsMessagesStub, items);
      sinon.assert.calledOnce(deleteItemsStub);
      sinon.assert.calledWith(deleteItemsStub, ['1'], sinon.match.string);

      sinon.assert.calledWith(consoleLogStub, sinon.match(/Found 2 items for deliveryDate/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 1 messages/));
      sinon.assert.calledWith(consoleErrorStub, sinon.match(/Not Sent 1 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 1 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Lambda execution completed successfully./));

      consoleLogStub.restore();
    });

    it('sends messages successfully but error during delete', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
      const sendMessageResponse = {
        successes: ['1'],
        failures: ['2']
      };
      const unprocessedItems = [{DeleteRequest: { Key: { deliveryDate : {S:"2025-07-07T00:00:00.000Z"}, requestId: {S:"1"} } }}];

      getItemsChunkStub.resolves({ items, lastKey: null });
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);
      deleteItemsStub.resolves(unprocessedItems);

      const consoleLogStub = sinon.stub(console, 'log');
      const consoleErrorStub = sinon.stub(console, 'error');

      await handleEvent();

      sinon.assert.calledOnce(getItemsChunkStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledWith(prepareAndSendSqsMessagesStub, items);
      sinon.assert.calledOnce(deleteItemsStub);
      sinon.assert.calledWith(deleteItemsStub, ['1'], sinon.match.string);

      sinon.assert.calledWith(consoleLogStub, sinon.match(/Found 2 items for deliveryDate/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 1 messages/));
      sinon.assert.calledWith(consoleErrorStub, sinon.match(/Not Sent 1 messages/));
      sinon.assert.calledWith(consoleErrorStub, sinon.match(/Failed to delete the following items:/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Lambda execution completed successfully./));

      consoleLogStub.restore();
    });


    it('sends messages and deletes successfully all items', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
      const sendMessageResponse = {
        successes: ['1','2'],
        failures: []
      };
      const unprocessedItems = [];

      getItemsChunkStub.resolves({ items, lastKey: null });
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);
      deleteItemsStub.resolves(unprocessedItems);

      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.calledOnce(getItemsChunkStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledWith(prepareAndSendSqsMessagesStub, items);
      sinon.assert.calledOnce(deleteItemsStub);
      sinon.assert.calledWith(deleteItemsStub, ['1','2'], sinon.match.string);

      sinon.assert.calledWith(consoleLogStub, sinon.match(/Found 2 items for deliveryDate/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 2 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 2 items/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Lambda execution completed successfully./));

      consoleLogStub.restore();
    });

    it('logs when no items are deleted due to no successful messages', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
      const sendMessageResponse = {
        successes: [],
        failures: ['1','2']
      };

      getItemsChunkStub.resolves({ items, lastKey: null });
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);

      const consoleLogStub = sinon.stub(console, 'log');
      const consoleErrorStub = sinon.stub(console, 'error');

      await handleEvent();

      sinon.assert.calledOnce(getItemsChunkStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledWith(prepareAndSendSqsMessagesStub, items);
      sinon.assert.notCalled(deleteItemsStub);

      sinon.assert.calledWith(consoleErrorStub, sinon.match(/Not Sent 2 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/No items to delete as no messages were successfully sent/));

      consoleLogStub.restore();
    });

    it('throws an error when getItemsChunk fails', async () => {
      getItemsChunkStub.rejects(new Error('DynamoDB error'));

      await expect(handleEvent()).to.be.rejectedWith('DynamoDB error');

      sinon.assert.calledOnce(getItemsChunkStub);
      sinon.assert.notCalled(prepareAndSendSqsMessagesStub);
      sinon.assert.notCalled(deleteItemsStub);
    });

    it('throws an error when prepareAndSendSqsMessages fails', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];

      getItemsChunkStub.resolves({ items, lastKey: null });
      prepareAndSendSqsMessagesStub.rejects(new Error('SQS error'));

      await expect(handleEvent()).to.be.rejectedWith('SQS error');

      sinon.assert.calledOnce(getItemsChunkStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.notCalled(deleteItemsStub);
    });

    it('throws an error when deleteItems fails', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
      const sendMessageResponse = {
        successes: ['1'],
        failures: ['2']
      };

      getItemsChunkStub.resolves({ items, lastKey: null });
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);
      deleteItemsStub.rejects(new Error('DynamoDB delete error'));

      await expect(handleEvent()).to.be.rejectedWith('DynamoDB delete error');

      sinon.assert.calledOnce(getItemsChunkStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledOnce(deleteItemsStub);
    });

    it('processes multiple chunks when lastKey is present', async () => {
        const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];

        const firstChunk = {
            items,
            lastKey: { requestId: {S:'3' }, deliveryDate: {S:'2023-10-01T00:00:00.000Z' }},
        };
        const secondChunk = {
            items,
            lastKey: null, 
        };
        const sendMessageResponse = {
            successes: ['1', '2'],
            failures: [],
        };
        const unprocessedItems = [];
    
        getItemsChunkStub.onFirstCall().resolves(firstChunk);
        getItemsChunkStub.onSecondCall().resolves(secondChunk);
        prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);
        deleteItemsStub.resolves(unprocessedItems);
    
        const consoleLogStub = sinon.stub(console, 'log');
    
        await handleEvent();
    
        sinon.assert.calledTwice(getItemsChunkStub);
        sinon.assert.calledTwice(prepareAndSendSqsMessagesStub);
        sinon.assert.calledWith(prepareAndSendSqsMessagesStub, firstChunk.items);
        sinon.assert.calledWith(prepareAndSendSqsMessagesStub, secondChunk.items);
    
        sinon.assert.calledTwice(deleteItemsStub);
        sinon.assert.calledWith(deleteItemsStub, ['1', '2'], sinon.match.string);
    
        sinon.assert.calledWith(consoleLogStub,sinon.match(/Found 2 items for deliveryDate/));
        sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 2 messages/));
        sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 2 items/));
        sinon.assert.calledWith(consoleLogStub, sinon.match(/Processing next chunk/));
        sinon.assert.calledWith(consoleLogStub,sinon.match(/Found 2 items for deliveryDate/));
        sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 2 messages/));
        sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 2 items/));
        sinon.assert.calledWith(consoleLogStub, sinon.match(/Lambda execution completed successfully./));

    
        consoleLogStub.restore();
    });
        it('calculates the correct deliveryDate based on DAY_TO_RECOVERY', async () => {
        process.env.PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE = "2025-05-06T00:00:00.000Z";
    
        getItemsChunkStub.resolves({ items: [], lastKey: null }); 
        const consoleLogStub = sinon.stub(console, 'log');
    
        await handleEvent();
    
        sinon.assert.calledOnce(getItemsChunkStub);
        sinon.assert.calledWith(getItemsChunkStub, "2025-05-06T00:00:00.000Z");
    
        consoleLogStub.restore();
        delete process.env.PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE;
    
        const expectedDate = new Date(Date.UTC(
            new Date().getUTCFullYear(),
            new Date().getUTCMonth(),
            new Date().getUTCDate() - 1
        )).toISOString();
    
        getItemsChunkStub.resetHistory();
        consoleLogStub.resetHistory();
    
        await handleEvent();
    
        sinon.assert.calledOnce(getItemsChunkStub);
        sinon.assert.calledWith(getItemsChunkStub, expectedDate, sinon.match.any);
    
        consoleLogStub.restore();
    });

});