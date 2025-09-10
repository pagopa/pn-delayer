const { expect } = require("chai");
const proxyquire = require("proxyquire").noPreserveCache();
const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
const {
  mockClient,
} = require("aws-sdk-client-mock");

describe("Lambda Handler Tests", () => {
  process.env.REGION = "us-east-1";
  process.env.KINESIS_PAPERDELIVERY_TABLE = 'TestIncomingTable';
  process.env.KINESIS_PAPERDELIVERY_EVENTTABLE = "PaperDeliveryKinesisEventTable";
  process.env.KINESIS_PAPERDELIVERY_COUNTERTABLE = 'TestCounterTable';
  
  const mockDynamoDBClient = mockClient(DynamoDBDocumentClient);

  const lambda = proxyquire.noCallThru().load("../app/eventHandler.js", {
    "@aws-sdk/client-dynamodb": {
      DynamoDBClient: mockDynamoDBClient
    },
    "../app/lib/kinesis": {
      extractKinesisData: (event) => event.mockKinesisData || [], 
    }
  });

  beforeEach(() => {
    mockDynamoDBClient.reset();// Reset to simulate success
  });

  it("should handle empty event data", async () => {
    const event = { mockKinesisData: [] };
    const result = await lambda.handleEvent(event);
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

  it("should handle a valid Kinesis event", async () => {
    mockDynamoDBClient
      .resolvesOnce({ Responses:{}  })
      .resolvesOnce({ Responses:{} })
      .resolvesOnce({ UnprocessedItems: {} })
      .resolvesOnce({ UnprocessedItems: {} });

    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'RM', cap: '12345', region: 'region1' },
              requestId: 'request1',
              productType: 'RS',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'RM', cap: '54321', region: 'region2' },
              requestId: 'request2',
              productType: 'RS',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

  it("should skip already processedEvent", async () => {
    mockDynamoDBClient
      .resolvesOnce({ Responses: {"PaperDeliveryKinesisEventTable":[{"requestId":"1234567890"}]} })
      .resolvesOnce({ UnprocessedItems: {} })
      .resolvesOnce({ UnprocessedItems: {} })

    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'RM', cap: '12345', region: 'region2' },
              requestId: 'request1',
              productType: 'AR',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'RM', cap: '54321', region: 'region2' },
              requestId: 'request2',
              productType: 'AR',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

  it("should skip all - already processedEvent", async () => {
    mockDynamoDBClient.resolvesOnce({ Responses: {PaperDeliveryKinesisEventTable:[{"requestId":"1234567890"}, {"requestId":"1234567891"}]} });

    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'address1', cap: '12345', region: 'region2' },
              requestId: '1234567890',
              productType: 'type1',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'address2', cap: '54321', region: 'region2' },
              requestId: '1234567891',
              productType: 'type2',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt:'0'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

  it("should handle DynamoDB operation failure on counter update", async () => {
    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'address1', cap: '12345', region: 'region1' },
              requestId: 'request1',
              productType: 'RS',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        },
      ],
    };

    // Simulate a DynamoDB failure
    mockDynamoDBClient.resolvesOnce({ Responses:{}  }).rejectsOnce("Simulated DynamoDB Error");

    const result = await lambda.handleEvent(event);
    expect(result.batchItemFailures).to.deep.equal(
      [ { itemIdentifier: '1234567890' } ] 
    );
  });

  it("should handle DynamoDB operation failure on incoming write", async () => {
    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'address1', cap: '12345', region: 'region1' },
              requestId: 'request1',
              productType: 'RS',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        },
      ],
    };

    // Simulate a DynamoDB failure
    mockDynamoDBClient.resolvesOnce({ Responses:{}  }).resolvesOnce({}).rejectsOnce("Simulated DynamoDB Error");

    const result = await lambda.handleEvent(event);
    expect(result.batchItemFailures).to.deep.equal(
      [ { itemIdentifier: '1234567890' } ] 
    );
  });


  it("should handle a valid Kinesis event with unprocessed item in update counter", async () => {
    mockDynamoDBClient.resolvesOnce({ Responses: {} })
    .rejectsOnce("Simulated DynamoDB Error")
    .resolvesOnce({})
    .resolvesOnce({ UnprocessedItems: {} })
    .resolvesOnce({ UnprocessedItems: {} })
   
    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'RM', cap: '12345', region: 'region1' },
              requestId: 'request1',
              productType: 'type1',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '1'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'RM', cap: '54321', region: 'region2' },
              requestId: 'request2',
              productType: 'type2',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver3',
              recipientNormalizedAddress: { pr: 'NA', cap: '67890', region: 'region3' },
              requestId: 'request3',
              productType: 'type3',
              senderPaId: 'sender3',
              tenderId: 'tender3',
              iun: 'iun3',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '1'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result.batchItemFailures).to.deep.equal(
      [ { itemIdentifier: '1234567890' } ] 
    );
  });

  it("should handle a valid Kinesis event with unprocessed item in incoming batch write", async () => {
    let tableName = process.env.KINESIS_PAPERDELIVERY_TABLE;
    mockDynamoDBClient.resolvesOnce({ Responses: {} })
    .resolvesOnce({ UnprocessedItems: {
      [tableName]: [
        { PutRequest: { Item: { sk: {'S':'RM~2023-10-01T00:00:00Z~request1' } } } }
      ]
    } })
    .resolvesOnce({ UnprocessedItems: {} });
   
    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'RM', cap: '12345', region: 'region1' },
              requestId: 'request1',
              productType: 'type1',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'RM', cap: '54321', region: 'region2' },
              requestId: 'request2',
              productType: 'type2',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        },
        {
              kinesisSeqNumber: '1234567892',
              unifiedDeliveryDriver: 'driver3',
              recipientNormalizedAddress: { pr: 'NA', cap: '67890', region: 'region3' },
              requestId: 'request3',
              productType: 'type3',
              senderPaId: 'sender3',
              tenderId: 'tender3',
              iun: 'iun3',
              notificationSentAt: '2023-10-01T00:00:00Z',
              prepareRequestDate: '2024-10-01T00:00:00Z',
              attempt: '0'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result.batchItemFailures).to.deep.equal(
      [ { itemIdentifier: '1234567890' }] 
    );
  });

    it("should skip without attempt", async () => {
        mockDynamoDBClient
            .resolvesOnce({ Responses:{}  })
            .resolvesOnce({ Responses:{} })
            .resolvesOnce({ UnprocessedItems: {} })
            .resolvesOnce({ UnprocessedItems: {} });

        const event = {
            mockKinesisData: [
                {
                    kinesisSeqNumber: '1234567890',
                    unifiedDeliveryDriver: 'driver1',
                    recipientNormalizedAddress: { pr: 'RM', cap: '12345', region: 'region2' },
                    requestId: 'request1',
                    productType: 'AR',
                    senderPaId: 'sender1',
                    tenderId: 'tender1',
                    iun: 'iun1',
                    notificationSentAt: '2023-10-01T00:00:00Z',
                    prepareRequestDate: '2024-10-01T00:00:00Z',
                    attempt: '0'
                },
                {
                    kinesisSeqNumber: '1234567891',
                    unifiedDeliveryDriver: 'driver2',
                    recipientNormalizedAddress: { pr: 'RM', cap: '54321', region: 'region2' },
                    requestId: 'request2',
                    productType: 'AR',
                    senderPaId: 'sender2',
                    tenderId: 'tender2',
                    iun: 'iun2',
                    notificationSentAt: '2023-10-01T00:00:00Z',
                    prepareRequestDate: '2024-10-01T00:00:00Z',
                }
            ],
        };

        const result = await lambda.handleEvent(event);
        expect(result.batchItemFailures).to.deep.equal(
            [ { itemIdentifier: '1234567890' }]
        );
    });

});
