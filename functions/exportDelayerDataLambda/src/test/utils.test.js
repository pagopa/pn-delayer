const { expect } = require('chai');
const { getCurrentMonday, prepareCsv, getAllElements } = require('../app/lib/utils');

describe('getCurrentMonday', () => {
  it('should return a string in YYYY-MM-DD format representing a Monday', () => {
    const monday = getCurrentMonday();
    const date = new Date(monday);
    expect(monday).to.match(/^\d{4}-\d{2}-\d{2}$/);
    expect(date.getDay()).to.equal(1); // 1 = Monday
  });
});

describe('prepareCsv', () => {
  it('should convert structured data into CSV format', () => {
    const input = [
      {
        unifiedDeliveryDriver: { S: 'Poste' },
        declaredCapacity: { N: '32000' },
        deliveryDate: { S: '2025-08-04' },
        geoKey: { S: 'RM' },
        usedCapacity: { N: '30' },
        unifiedDeliveryDriverGeokey: { S: 'Poste~RM' }
      },
      {
        unifiedDeliveryDriver: { S: 'Poste' },
        declaredCapacity: { N: '4000' },
        deliveryDate: { S: '2025-08-04' },
        geoKey: { S: '00139' },
        usedCapacity: { N: '30' },
        unifiedDeliveryDriverGeokey: { S: 'Poste~00139' }
      }
    ]

    const csv = prepareCsv(input);
    const expected = 'unifiedDeliveryDriver,declaredCapacity,deliveryDate,geoKey,usedCapacity,unifiedDeliveryDriverGeokey\nPoste,32000,2025-08-04,RM,30,Poste~RM\nPoste,4000,2025-08-04,00139,30,Poste~00139';

    expect(csv).to.equal(expected);
  });
});

describe('getAllElements', () => {
  it('should paginate and collect all items', async () => {
    let callCount = 0;

    async function mockFunction(arg1, token) {
      callCount++;
      if (!token) {
        return { Items: [{ id: 1 }], nextToken: 'token-1' };
      }
      if (token === 'token-1') {
        return { Items: [{ id: 2 }], nextToken: undefined };
      }
    }

    const result = await getAllElements(mockFunction, 'arg1');
    expect(result).to.be.an('array').with.lengthOf(2);
    expect(result[0].id).to.equal(1);
    expect(result[1].id).to.equal(2);
    expect(callCount).to.equal(2);
  });
});
