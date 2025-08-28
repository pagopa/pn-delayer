const { expect } = require('chai');
const proxyquire = require('proxyquire');

const mockItems = [
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
];

// Mock lib/dynamo
const dynamoMock = {
  queryRequestByIndex: async () => mockItems
};

// Mock lib/s3
const s3Mock = {
  putObject: async () => 'mock-etag',
  getpresigneUrlObject: async () => 'https://mock-url'
};

// Mock lib/sns
const snsMock = {
  publishToSnsTopic: async () => 'mock-message-id'
};

const utilsMock = {
  getCurrentMonday: () => '2025-08-04',
  prepareCsv: () => 'mock-csv'
};

// carichiamo eventHandler con i mock al posto delle lib AWS
const { handleEvent } = proxyquire('../app/eventHandler', {
  './lib/dynamo': dynamoMock,
  './lib/s3': s3Mock,
  './lib/sns': snsMock,
  './lib/utils': utilsMock
});

describe('event handler', () => {
  it('positive execution with mock response of aws', async () => {
    const response = await handleEvent();
    expect(response.statusCode).to.equal(200);
    expect(response.body).to.include('exported successfully');
  });


  
});
