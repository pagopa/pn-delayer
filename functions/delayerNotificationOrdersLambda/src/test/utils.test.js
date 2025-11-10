const fs = require('fs');
const path = require('path');
const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');

before(function() {
  this.timeout(20000);
});

describe('extractDataFromOrder', () => {
  let persistOrderRecordsStub, extractDataFromOrder;

  beforeEach(() => {
    persistOrderRecordsStub = sinon.stub().resolves();
    const utilsModule = proxyquire('../app/utils', {
      './dynamo': { persistOrderRecords: persistOrderRecordsStub }
    });
    extractDataFromOrder = utilsModule.extractDataFromOrder;
  });

  afterEach(() => {
    sinon.restore();
  });

  it('estrae record aggregati e regionali dal Modulo_Commessa_v4.json', async () => {
    const jsonPath = path.join(__dirname, 'Modulo_Commessa_v4.json');
    let order;
    order = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
    const fileKey = 'test-key';
    const records = await extractDataFromOrder(order, fileKey);

    expect(records).to.be.an('array').that.is.not.empty;
    expect(persistOrderRecordsStub.calledWith(records, fileKey)).to.be.true;

    expect(records.some(r =>
      typeof r.pk === 'string' &&
      r.sk.includes(order.idEnte) &&
      r.sk.split('_').length === 3
    )).to.be.true;

    expect(records.some(r =>
      r.sk.split('_').length === 4
    )).to.be.true;
  });

  it('gestisce commessa senza prodotti', async () => {
    const order = {
      periodo_riferimento: '06-2024',
      idEnte: 'ente1',
      prodotti: [],
      last_update: '2024-06-01'
    };
    const fileKey = 'empty-products';
    const records = await extractDataFromOrder(order, fileKey);

    expect(records).to.deep.equal([]);
    expect(persistOrderRecordsStub.called).to.be.false;
  });

  it('ritorna un array vuoto se la commessa è vuota, undefined o manca di campi essenziali', async () => {
      const fileKey = 'missing-data';
      let orderNull = null;
      let recordsNull = await extractDataFromOrder(orderNull, fileKey);
      expect(recordsNull).to.deep.equal([]);
      expect(persistOrderRecordsStub.called).to.be.false;

      let orderUndefined = undefined;
      let recordsUndefined = await extractDataFromOrder(orderUndefined, fileKey);
      expect(recordsUndefined).to.deep.equal([]);
      expect(persistOrderRecordsStub.callCount).to.equal(0);

      const orderNoProdotti = {
        periodo_riferimento: '06-2024',
        idEnte: 'ente1',
        last_update: '2024-06-01'
      };
      let recordsNoProdotti = await extractDataFromOrder(orderNoProdotti, fileKey);
      expect(recordsNoProdotti).to.deep.equal([]);
      expect(persistOrderRecordsStub.callCount).to.equal(0);

      const orderNoIdEnte = {
        periodo_riferimento: '06-2024',
        prodotti: [],
        last_update: '2024-06-01'
      };
      let recordsNoIdEnte = await extractDataFromOrder(orderNoIdEnte, fileKey);
      expect(recordsNoIdEnte).to.deep.equal([]);
      expect(persistOrderRecordsStub.callCount).to.equal(0);

      const orderNoPeriodo = {
        idEnte: 'ente1',
        prodotti: [],
        last_update: '2024-06-01'
      };
      let recordsNoPeriodo = await extractDataFromOrder(orderNoPeriodo, fileKey);
      expect(recordsNoPeriodo).to.deep.equal([]);
      expect(persistOrderRecordsStub.callCount).to.equal(0);

      expect(persistOrderRecordsStub.callCount).to.equal(0);
    });

  it('gestisce prodotto senza varianti', async () => {
    const order = {
      periodo_riferimento: '06-2024',
      idEnte: 'ente1',
      prodotti: [{ id: 'prod1', varianti: [] }],
      last_update: '2024-06-01'
    };
    const fileKey = 'no-variants';
    const records = await extractDataFromOrder(order, fileKey);

    expect(records).to.deep.equal([]);
    expect(persistOrderRecordsStub.called).to.be.false;
  });

  it('gestisce variante senza distribuzione regionale', async () => {
    const order = {
      periodo_riferimento: '06-2024',
      idEnte: 'ente1',
      prodotti: [{
        id: 'prod1',
        varianti: [{
          codice: 'VAR1',
          valore_totale: 123,
          distribuzione: null
        }]
      }],
      last_update: '2024-06-01'
    };
    const fileKey = 'no-regional';
    const records = await extractDataFromOrder(order, fileKey);

    expect(records).to.have.lengthOf(1);
    expect(records[0].pk).to.equal('2024-06-01');
    expect(records[0].sk).to.equal('ente1_prod1_VAR1');
    expect(records[0].value).to.equal(123);

    expect(persistOrderRecordsStub.calledWith(records, fileKey)).to.be.true;
  });

  it('gestisce variante con distribuzione regionale', async () => {
    const order = {
      periodo_riferimento: '06-2024',
      idEnte: 'ente1',
      prodotti: [{
        id: 'prod1',
        varianti: [{
          codice: 'VAR1',
          valore_totale: 123,
          distribuzione: {
            regionale: [
              { regione: 'LZ', valore: 50 },
              { regione: 'PI', valore: 73 }
            ]
          }
        }]
      }],
      last_update: '2024-06-01'
    };
    const fileKey = 'with-regional';
    const records = await extractDataFromOrder(order, fileKey);

    expect(records).to.have.lengthOf(3);

    const basePk = records[0].pk;

    const simplified = records.map(r => ({
      pk: r.pk,
      sk: r.sk,
      value: r.value,
      fileKey: r.fileKey
    }));

    expect(simplified).to.deep.include.members([
      { pk: basePk, sk: 'ente1_prod1_VAR1', value: 123, fileKey },
      { pk: basePk, sk: 'ente1_prod1_VAR1_LZ', value: 50, fileKey },
      { pk: basePk, sk: 'ente1_prod1_VAR1_PI', value: 73, fileKey }
    ]);

    expect(persistOrderRecordsStub.calledWith(records, fileKey)).to.be.true;
  });
});
