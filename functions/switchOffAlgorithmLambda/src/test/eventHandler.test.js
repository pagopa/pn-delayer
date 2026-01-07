const proxyquire = require('proxyquire').noCallThru();
const sinon = require('sinon');
const { expect } = require('chai');

describe('eventHandler.js', () => {
  let handler;

  let queryByPartitionKeyStub;
  let insertItemsBatchStub;
  let buildPaperDeliveryRecordStub;

  let LocalDateStub;
  let DayOfWeekStub;
  let TemporalAdjustersStub;

  beforeEach(() => {
    process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME = 'test-table';
    process.env.QUERY_LIMIT = '1000';
    process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK = '1';

    queryByPartitionKeyStub = sinon.stub();
    insertItemsBatchStub = sinon.stub();
    buildPaperDeliveryRecordStub = sinon.stub();

    const fakeDateWeek1 = {
      toString: () => '2026-01-05',
      plusWeeks: (n) => {
        if (n !== 1) throw new Error('Unexpected plusWeeks arg');
        return {
          toString: () => '2026-01-12',
          plusWeeks: () => {
            throw new Error('Not needed in tests');
          },
        };
      },
      with: function () {
        return this;
      },
    };

    LocalDateStub = {
      now: sinon.stub().returns(fakeDateWeek1),
    };

    DayOfWeekStub = {
      of: sinon.stub().callsFake((n) => n),
    };

    TemporalAdjustersStub = {
      next: sinon.stub().callsFake((dow) => ({ dow })),
    };

    handler = proxyquire('../app/eventHandler', {
      '@js-joda/core': {
        LocalDate: LocalDateStub,
        DayOfWeek: DayOfWeekStub,
        TemporalAdjusters: TemporalAdjustersStub,
      },
      './dynamo': {
        queryByPartitionKey: queryByPartitionKeyStub,
        insertItemsBatch: insertItemsBatchStub,
      },
      './utils': {
        buildPaperDeliveryRecord: buildPaperDeliveryRecordStub,
      },
    });
  });

  afterEach(() => {
    sinon.restore();
    delete process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME;
    delete process.env.QUERY_LIMIT;
    delete process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK;
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

    const expectedPk = '2026-01-05~EVALUATE_SENDER_LIMIT';

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
    expect(queryByPartitionKeyStub.getCall(0).args[0]).to.equal('2026-01-05~EVALUATE_SENDER_LIMIT');
    expect(queryByPartitionKeyStub.getCall(1).args[0]).to.equal('2026-01-12~EVALUATE_SENDER_LIMIT');

    expect(res.itemsProcessed).to.equal(2);
    expect(res.completed).to.equal(true);
  });

});
