const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');

describe('handleEvent', () => {
  let getDeliveryWeekDateStub;
  let retrieveProvinceWithPaperDeliveriesStub;
  let handleEvent;

  beforeEach(() => {
    getDeliveryWeekDateStub = sinon.stub();
    retrieveProvinceWithPaperDeliveriesStub = sinon.stub();

    handleEvent = proxyquire('../app/eventHandler', {
      './lib/utils': { getDeliveryWeek: getDeliveryWeekDateStub },
      './lib/dynamo': { retrieveProvinceWithPaperDeliveries: retrieveProvinceWithPaperDeliveriesStub }
    }).handleEvent;
  });

  afterEach(() => {
    sinon.restore();
  });

  it('return correct deliveryWeek and provinces', async () => {
    getDeliveryWeekDateStub.returns('2024-06-10');
    retrieveProvinceWithPaperDeliveriesStub.resolves([
      { sk: 'EVAL~MI' },
      { sk: 'EVAL~RM' },
      { sk: 'EVAL~TO' }
    ]);

    const result = await handleEvent({});

    expect(result).to.deep.equal({
      deliveryWeek: '2024-06-10',
      provinces: ['MI', 'RM', 'TO']
    });
  });

  it('handle if no items are found', async () => {
    getDeliveryWeekDateStub.returns('2024-06-10');
    retrieveProvinceWithPaperDeliveriesStub.resolves([]);

    const result = await handleEvent({});

    expect(result).to.deep.equal({
      deliveryWeek: '2024-06-10',
      provinces: []
    });
  });

  it('ignore sk which do not start with "EVAL"', async () => {
    getDeliveryWeekDateStub.returns('2024-06-10');
    retrieveProvinceWithPaperDeliveriesStub.resolves([
      { sk: 'EVAL~MI' },
      { sk: 'ALTRO~BO' },
      { sk: null }
    ]);

    const result = await handleEvent({});

    expect(result).to.deep.equal({
      deliveryWeek: '2024-06-10',
      provinces: ['MI']
    });
  });
});