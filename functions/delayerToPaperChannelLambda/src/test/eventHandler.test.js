const sinon = require('sinon');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const { handleEvent } = require('../app/eventHandler');
const dynamo = require('../app/lib/dynamo');
const sqsSender = require('../app/lib/sqsSender');
chai.use(chaiAsPromised);
const { expect } = chai;

let getItemsStub, deleteItemsStub, prepareAndSendSqsMessagesStub;

beforeEach(() => {
    getItemsStub = sinon.stub(dynamo, 'getItems');
    deleteItemsStub = sinon.stub(dynamo, 'deleteItems');
    prepareAndSendSqsMessagesStub = sinon.stub(sqsSender, 'prepareAndSendSqsMessages');
});

afterEach(() => {
    sinon.restore();
});

describe('handleEvent', () => {
    it('logs and exits when no items are found', async () => {
      getItemsStub.resolves([]);
      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.calledOnce(getItemsStub);
      sinon.assert.calledWith(consoleLogStub, sinon.match(/No items found for deliveryDate/));
      sinon.assert.notCalled(prepareAndSendSqsMessagesStub);
      sinon.assert.notCalled(deleteItemsStub);

      consoleLogStub.restore();
    });

    it('sends messages and deletes successfully processed items', async () => {
      const items = [{ id: '1' }, { id: '2' }];
      const sendMessageResponse = {
        successes: ['1'],
        failures: ['2']
      };
      const unprocessedItems = [];

      getItemsStub.resolves(items);
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);
      deleteItemsStub.resolves(unprocessedItems);

      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.calledOnce(getItemsStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledWith(prepareAndSendSqsMessagesStub, items);
      sinon.assert.calledOnce(deleteItemsStub);
      sinon.assert.calledWith(deleteItemsStub, ['1'], sinon.match.string);

      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 1 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 1 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 1 items/));

      consoleLogStub.restore();
    });

    it('sends messages and deletes successfully all items', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
      const sendMessageResponse = {
        successes: ['1','2'],
        failures: []
      };
      const unprocessedItems = [];

      getItemsStub.resolves(items);
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);
      deleteItemsStub.resolves(unprocessedItems);

      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.calledOnce(getItemsStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledWith(prepareAndSendSqsMessagesStub, items);
      sinon.assert.calledOnce(deleteItemsStub);
      sinon.assert.calledWith(deleteItemsStub, ['1','2'], sinon.match.string);

      sinon.assert.calledWith(consoleLogStub, sinon.match(/Sent 2 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Not Sent 0 messages/));
      sinon.assert.calledWith(consoleLogStub, sinon.match(/Deleted 2 items/));

      consoleLogStub.restore();
    });

    it('logs when no items are deleted due to no successful messages', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
      const sendMessageResponse = {
        successes: [],
        failures: ['1','2']
      };

      getItemsStub.resolves(items);
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);

      const consoleLogStub = sinon.stub(console, 'log');

      await handleEvent();

      sinon.assert.calledOnce(getItemsStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledWith(prepareAndSendSqsMessagesStub, items);
      sinon.assert.notCalled(deleteItemsStub);

      sinon.assert.calledWith(consoleLogStub, sinon.match(/No items to delete as no messages were successfully sent/));

      consoleLogStub.restore();
    });

    it('throws an error when getItems fails', async () => {
      getItemsStub.rejects(new Error('DynamoDB error'));

      await expect(handleEvent()).to.be.rejectedWith('DynamoDB error');

      sinon.assert.calledOnce(getItemsStub);
      sinon.assert.notCalled(prepareAndSendSqsMessagesStub);
      sinon.assert.notCalled(deleteItemsStub);
    });

    it('throws an error when prepareAndSendSqsMessages fails', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];

      getItemsStub.resolves(items);
      prepareAndSendSqsMessagesStub.rejects(new Error('SQS error'));

      await expect(handleEvent()).to.be.rejectedWith('SQS error');

      sinon.assert.calledOnce(getItemsStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.notCalled(deleteItemsStub);
    });

    it('throws an error when deleteItems fails', async () => {
      const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
      const sendMessageResponse = {
        successes: ['1'],
        failures: ['2']
      };

      getItemsStub.resolves(items);
      prepareAndSendSqsMessagesStub.resolves(sendMessageResponse);
      deleteItemsStub.rejects(new Error('DynamoDB delete error'));

      await expect(handleEvent()).to.be.rejectedWith('DynamoDB delete error');

      sinon.assert.calledOnce(getItemsStub);
      sinon.assert.calledOnce(prepareAndSendSqsMessagesStub);
      sinon.assert.calledOnce(deleteItemsStub);
    });
});