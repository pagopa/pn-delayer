const proxyquire = require('proxyquire').noCallThru();
const sinon = require('sinon');
const { expect } = require('chai');
const { LocalDate, TemporalAdjusters, DayOfWeek } = require('@js-joda/core');

describe('eventHandler.js', () => {
  let handler;

  let queryByPartitionKeyStub;
  let insertItemsBatchStub;
  let buildPaperDeliveryRecordStub;

  let localDateNowStub = LocalDate.now().with(TemporalAdjusters.previousOrSame(DayOfWeek.of(1)));

  beforeEach(() => {
    process.env.QUERY_LIMIT = '1000';
    process.env.DELIVERYDATEDAYOFWEEK = '1';

    queryByPartitionKeyStub = sinon.stub();
    insertItemsBatchStub = sinon.stub();
    buildPaperDeliveryRecordStub = sinon.stub();

    handler = proxyquire('../app/eventHandler', {
      './lib/dynamo': {
        queryByPartitionKey: queryByPartitionKeyStub,
        insertItemsBatch: insertItemsBatchStub,
      },
      './lib/utils': {
        buildPaperDeliveryRecord: buildPaperDeliveryRecordStub,
      },
    });
  });

  afterEach(() => {
    sinon.restore();
    delete process.env.DELIVERYDATEDAYOFWEEK;
  });

  it('handleEvent lancia errore se executionLimit mancante o <= 0', async () => {
    try {
      await handler.handleEvent({});
      throw new Error('Expected error not thrown');
    } catch (err) {
      expect(err.message).to.equal('executionLimit è obbligatorio e deve essere maggiore di 0');
    }

    try {
      await handler.handleEvent({ executionLimit: 0 });
      throw new Error('Expected error not thrown');
    } catch (err) {
      expect(err.message).to.equal('executionLimit è obbligatorio e deve essere maggiore di 0');
    }
  });

  it('handleEvent: processa items, fa batch write, completed=true se non c’è lastEvaluatedKey', async () => {
    process.env.DELAY_SECONDS = '30';
    queryByPartitionKeyStub.resolves({
      items: [{ id: 1 }, { id: 2 }],
      lastEvaluatedKey: null,
    });

    buildPaperDeliveryRecordStub.callsFake((item, deliveryDate) => ({
      ...item,
      pkBuiltFrom: `${deliveryDate}`,
    }));

    insertItemsBatchStub.resolves([]);

    const res = await handler.handleEvent({ executionLimit: 10, lastEvaluatedKey: null });

    const expectedPk = localDateNowStub + '~EVALUATE_SENDER_LIMIT';

    expect(queryByPartitionKeyStub.calledTwice).to.equal(true);
    expect(queryByPartitionKeyStub.firstCall.args[0]).to.equal(expectedPk);
    expect(queryByPartitionKeyStub.firstCall.args[1]).to.equal(10); // executionLimit - processedCount(0)
    expect(queryByPartitionKeyStub.firstCall.args[2]).to.equal(null);

    expect(buildPaperDeliveryRecordStub.callCount).to.equal(4);
    expect(insertItemsBatchStub.calledTwice).to.equal(true);

    expect(res).to.deep.equal({
      success: true,
      itemsProcessed: 4,
      lastEvaluatedKey: null,
      completed: true,
      delaySeconds: 30,
    });
  });

  it('handleEvent: se insertItemsBatch ritorna unprocessed -> lancia errore', async () => {
    queryByPartitionKeyStub.resolves({
      items: [{ id: 1 }],
      lastEvaluatedKey: null,
    });

    buildPaperDeliveryRecordStub.returns({ id: 1 });

    insertItemsBatchStub.resolves([{ PutRequest: { Item: { id: 1 } } }]);

    try {
      await handler.handleEvent({ executionLimit: 10 });
      throw new Error('Expected error not thrown');
    } catch (err) {
      expect(err.message).to.match(/Batch write failed: 1 unprocessed items/);
    }
  });

  it('handleEvent: paginazione (lastEvaluatedKey presente) richiama query più volte finché può', async () => {
    queryByPartitionKeyStub.onCall(0).resolves({
      items: [{ id: 1 }],
      lastEvaluatedKey: { pk: 'lek1' },
    });
    queryByPartitionKeyStub.onCall(1).resolves({
      items: [{ id: 2 }],
      lastEvaluatedKey: { pk: 'lek2' },
    });
    queryByPartitionKeyStub.onCall(2).resolves({
      items: [{ id: 3 }],
      lastEvaluatedKey: { pk: 'lek3' },
    });

    buildPaperDeliveryRecordStub.callsFake((item) => item);
    insertItemsBatchStub.resolves([]);

    const res = await handler.handleEvent({ executionLimit: 3, lastEvaluatedKey: null });

    expect(queryByPartitionKeyStub.callCount).to.equal(3);

    expect(queryByPartitionKeyStub.getCall(0).args[1]).to.equal(3);
    expect(queryByPartitionKeyStub.getCall(0).args[2]).to.equal(null);

    expect(queryByPartitionKeyStub.getCall(1).args[1]).to.equal(2);
    expect(queryByPartitionKeyStub.getCall(1).args[2]).to.deep.equal({ pk: 'lek1' });

    expect(queryByPartitionKeyStub.getCall(2).args[1]).to.equal(1);
    expect(queryByPartitionKeyStub.getCall(2).args[2]).to.deep.equal({ pk: 'lek2' });

    expect(res.success).to.equal(true);
    expect(res.itemsProcessed).to.equal(3);
    expect(res.lastEvaluatedKey).to.deep.equal({ pk: 'lek3' });
    expect(res.completed).to.equal(false);
  });

  it('handleEvent: quando week1 finisce e c’è ancora spazio, processa week2', async () => {
    queryByPartitionKeyStub.onCall(0).resolves({
      items: [{ id: 1 }],
      lastEvaluatedKey: null,
    });
    queryByPartitionKeyStub.onCall(1).resolves({
      items: [{ id: 2 }],
      lastEvaluatedKey: null,
    });

    buildPaperDeliveryRecordStub.callsFake((i) => i);
    insertItemsBatchStub.resolves([]);

    const res = await handler.handleEvent({ executionLimit: 10, lastEvaluatedKey: null });

    expect(queryByPartitionKeyStub.callCount).to.equal(2);
    expect(queryByPartitionKeyStub.getCall(0).args[0]).to.equal(localDateNowStub + '~EVALUATE_SENDER_LIMIT');
    expect(queryByPartitionKeyStub.getCall(1).args[0]).to.equal(localDateNowStub.plusWeeks(1) + '~EVALUATE_SENDER_LIMIT');

    expect(res.itemsProcessed).to.equal(2);
    expect(res.completed).to.equal(true);
  });

});
