const proxyquire = require('proxyquire').noCallThru();
const sinon = require('sinon');
const { expect } = require('chai');

describe('eventHandler.js', () => {
  let handler;

  let queryLatestByRequestIdStub;
  let insertItemsBatchStub;
  let moveItemsToDeletedStub;
  let buildPaperDeliveryRecordStub;

  beforeEach(() => {
    queryLatestByRequestIdStub = sinon.stub();
    insertItemsBatchStub = sinon.stub();
    moveItemsToDeletedStub = sinon.stub();
    buildPaperDeliveryRecordStub = sinon.stub();

    handler = proxyquire('../app/eventHandler', {
      './lib/dynamo': {
        queryLatestByRequestId: queryLatestByRequestIdStub,
        insertItemsBatch: insertItemsBatchStub,
        moveItemsToDeleted: moveItemsToDeletedStub
      },
      './lib/utils': {
        buildPaperDeliveryRecord: buildPaperDeliveryRecordStub
      }
    });
  });

  afterEach(() => {
    sinon.restore();
  });

  it('handleEvent lancia errore se requestIds mancante o vuoto', async () => {
    for (const event of [{}, { requestIds: [] }, { requestIds: 'abc' }]) {
      try {
        await handler.handleEvent(event);
        throw new Error('Expected error not thrown');
      } catch (err) {
        expect(err.message).to.equal('requestIds è obbligatorio e deve essere una lista non vuota');
      }
    }
  });

  it('handleEvent processa solo gli item in EVALUATE_SENDER_LIMIT', async () => {
    const sourceItem = {
      pk: 'pk1',
      sk: 'sk1',
      requestId: 'request-id-1',
      workflowStep: 'EVALUATE_SENDER_LIMIT',
      deliveryDate: '2026-07-06'
    };

    const builtItem = {
      pk: '2026-07-06~SENT_TO_PREPARE_PHASE_2',
      sk: '2026-07-06~request-id-1',
      requestId: 'request-id-1',
      workflowStep: 'SENT_TO_PREPARE_PHASE_2'
    };

    queryLatestByRequestIdStub.resolves(sourceItem);
    buildPaperDeliveryRecordStub.returns(builtItem);
    insertItemsBatchStub.resolves([]);
    moveItemsToDeletedStub.resolves();

    const result = await handler.handleEvent({
      requestIds: ['request-id-1']
    });

    expect(queryLatestByRequestIdStub.calledOnceWithExactly('request-id-1')).to.equal(true);
    expect(buildPaperDeliveryRecordStub.calledOnceWithExactly(sourceItem)).to.equal(true);

    expect(insertItemsBatchStub.calledOnce).to.equal(true);
    expect(insertItemsBatchStub.firstCall.args[0]).to.deep.equal([
      {
        PutRequest: {
          Item: builtItem
        }
      }
    ]);

    expect(moveItemsToDeletedStub.calledOnceWithExactly([sourceItem])).to.equal(true);
    expect(result).to.deep.equal([]);
  });

  it('handleEvent skippa requestId senza item', async () => {
    queryLatestByRequestIdStub.resolves(null);
    insertItemsBatchStub.resolves([]);
    moveItemsToDeletedStub.resolves();

    const result = await handler.handleEvent({
      requestIds: ['request-id-1']
    });

    expect(queryLatestByRequestIdStub.calledOnceWithExactly('request-id-1')).to.equal(true);
    expect(buildPaperDeliveryRecordStub.notCalled).to.equal(true);
    expect(insertItemsBatchStub.notCalled).to.equal(true);
    expect(moveItemsToDeletedStub.notCalled).to.equal(true);
    expect(result).to.deep.equal(['request-id-1']);
  });

  it('handleEvent skippa requestId se workflowStep non è EVALUATE_SENDER_LIMIT', async () => {
    const sourceItem = {
      pk: 'pk1',
      sk: 'sk1',
      requestId: 'request-id-1',
      workflowStep: 'OTHER_STEP'
    };

    queryLatestByRequestIdStub.resolves(sourceItem);

    const result = await handler.handleEvent({
      requestIds: ['request-id-1']
    });

    expect(queryLatestByRequestIdStub.calledOnceWithExactly('request-id-1')).to.equal(true);
    expect(buildPaperDeliveryRecordStub.notCalled).to.equal(true);
    expect(insertItemsBatchStub.notCalled).to.equal(true);
    expect(moveItemsToDeletedStub.notCalled).to.equal(true);
    expect(result).to.deep.equal(['request-id-1']);
  });

  it('handleEvent processa più requestId e ritorna quelli skippati', async () => {
    const validItem = {
      pk: 'pk1',
      sk: 'sk1',
      requestId: 'request-id-1',
      workflowStep: 'EVALUATE_SENDER_LIMIT'
    };

    const invalidItem = {
      pk: 'pk2',
      sk: 'sk2',
      requestId: 'request-id-2',
      workflowStep: 'SENT_TO_PREPARE_PHASE_2'
    };

    const builtItem = {
      pk: 'new-pk',
      sk: 'new-sk',
      requestId: 'request-id-1'
    };

    queryLatestByRequestIdStub.onCall(0).resolves(validItem);
    queryLatestByRequestIdStub.onCall(1).resolves(invalidItem);
    queryLatestByRequestIdStub.onCall(2).resolves(null);

    buildPaperDeliveryRecordStub.returns(builtItem);
    insertItemsBatchStub.resolves([]);
    moveItemsToDeletedStub.resolves();

    const result = await handler.handleEvent({
      requestIds: ['request-id-1', 'request-id-2', 'request-id-3']
    });

    expect(queryLatestByRequestIdStub.callCount).to.equal(3);
    expect(insertItemsBatchStub.calledOnce).to.equal(true);
    expect(moveItemsToDeletedStub.calledOnceWithExactly([validItem])).to.equal(true);

    expect(result).to.deep.equal(['request-id-2', 'request-id-3']);
  });

  it('handleEvent lancia errore se insertItemsBatch ritorna unprocessed items', async () => {
    const sourceItem = {
      pk: 'pk1',
      sk: 'sk1',
      requestId: 'request-id-1',
      workflowStep: 'EVALUATE_SENDER_LIMIT'
    };

    const builtItem = {
      pk: 'new-pk',
      sk: 'new-sk'
    };

    queryLatestByRequestIdStub.resolves(sourceItem);
    buildPaperDeliveryRecordStub.returns(builtItem);
    insertItemsBatchStub.resolves([
      {
        PutRequest: {
          Item: builtItem
        }
      }
    ]);

    try {
      await handler.handleEvent({
        requestIds: ['request-id-1']
      });
      throw new Error('Expected error not thrown');
    } catch (err) {
      expect(err.message).to.equal('Batch write failed: 1 unprocessed items');
    }

    expect(moveItemsToDeletedStub.notCalled).to.equal(true);
  });
});