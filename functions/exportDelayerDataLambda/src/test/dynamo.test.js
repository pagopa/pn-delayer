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

class MockQueryCommand {
  constructor(input) {
    this.input = input;
  }
}

class MockDynamoDBClient {
  async send(command) {
    this.lastCommand = command;
    return {
      Items: mockItems,
      nextToken: undefined
    };
  }
}

describe('queryRequestByIndex', () => {
  // Usa proxyquire per sostituire DynamoDBClient e QueryCommand
  const { queryRequestByIndex } = proxyquire('../app/lib/dynamo', {
    '@aws-sdk/client-dynamodb': {
      DynamoDBClient: MockDynamoDBClient,
      QueryCommand: MockQueryCommand
    },
  });

  it('restituisce gli items attesi per deliveryDate = 2025-08-04', async () => {
    const result = await queryRequestByIndex(
      'pn-PaperDeliveryDriverUsedCapacities',
      'deliveryDate-index',
      'deliveryDate',
      '2025-08-04'
    );

    expect(result.Items).to.be.an('array').with.lengthOf(2);
    expect(result.Items[0].geoKey.S).to.equal('RM');
    expect(result.Items[1].geoKey.S).to.equal('00139');
    expect(result.Items[0].unifiedDeliveryDriverGeokey.S).to.equal('Poste~RM');
  });

  it('gestisce correttamente la presenza di lastEvaluatedKey', async () => {
    const result = await queryRequestByIndex(
      'pn-PaperDeliveryDriverUsedCapacities',
      'deliveryDate-index',
      'deliveryDate',
      '2025-08-04',
      { pk: { S: 'some-pk' }, sk: { S: 'some-sk' } }
    );

    expect(result.Items).to.be.an('array').with.lengthOf(2);
  });
});
