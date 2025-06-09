const { expect } = require("chai");
const proxyquire = require("proxyquire").noPreserveCache();
const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
const {
  mockClient,
} = require("aws-sdk-client-mock");

describe("Lambda Handler Tests", () => {
  process.env.REGION = "us-east-1";
  process.env.HIGH_PRIORITY_TABLE_NAME = "HighPriorityTable";
  process.env.KINESIS_PAPER_DELIVERY_EVENT_TABLE_NAME = "KinesisPaperDeliveryEventTable";
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
    mockDynamoDBClient.resolvesOnce({ Responses:{}  }).resolvesOnce({ UnprocessedItems: {} }).resolvesOnce({ UnprocessedItems: {} });
    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'address1', cap: '12345'},
              requestId: 'request1',
              productType: 'type1',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'address2', cap: '54321'},
              requestId: 'request2',
              productType: 'type2',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

    it("should skip already processedEvent", async () => {
    mockDynamoDBClient.resolvesOnce({ Responses: {"KinesisPaperDeliveryEventTable":[{"sequenceNumber":"1234567890"}]} }).resolvesOnce({ UnprocessedItems: {} }).resolvesOnce({ UnprocessedItems: {} })
    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'address1', cap: '12345'},
              requestId: 'request1',
              productType: 'type1',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'address2', cap: '54321'},
              requestId: 'request2',
              productType: 'type2',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

  it("should skip all - already processedEvent", async () => {
    mockDynamoDBClient.resolvesOnce({ Responses: {KinesisPaperDeliveryEventTable:[{"sequenceNumber":"1234567890"}, {"sequenceNumber":"1234567891"}]} })
    .resolvesOnce({ UnprocessedItems: {} }).resolvesOnce({ UnprocessedItems: {} })

    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'address1', cap: '12345'},
              requestId: 'request1',
              productType: 'type1',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'address2', cap: '54321'},
              requestId: 'request2',
              productType: 'type2',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

  it("should handle DynamoDB operation failure", async () => {
    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'address1', cap: '12345'},
              requestId: 'request1',
              productType: 'type1',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1'
          
        },
      ],
    };

    // Simulate a DynamoDB failure
    mockDynamoDBClient.resolvesOnce({ Responses:{}  }).rejectsOnce("Simulated DynamoDB Error").resolvesOnce({ UnprocessedItems: {} });

    const result = await lambda.handleEvent(event);
    expect(result.batchItemFailures).to.deep.equal(
      [ { itemIdentifier: '1234567890' } ] 
    );
  });

  it("should handle a valid Kinesis event with unprocessed item", async () => {
    let tableName = process.env.HIGH_PRIORITY_TABLE_NAME;
    mockDynamoDBClient.resolvesOnce({ Responses: {} })
    .resolvesOnce({ UnprocessedItems: {
      [tableName]: [
        { PutRequest: { Item: { requestId: {'S':'request1' } } } }
      ]
    } })
    .resolvesOnce({ UnprocessedItems: {} })
    const event = {
      mockKinesisData: [
        {
              kinesisSeqNumber: '1234567890',
              unifiedDeliveryDriver: 'driver1',
              recipientNormalizedAddress: { pr: 'address1', cap: '12345'},
              requestId: 'request1',
              productType: 'type1',
              senderPaId: 'sender1',
              tenderId: 'tender1',
              iun: 'iun1'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver2',
              recipientNormalizedAddress: { pr: 'address2', cap: '54321' },
              requestId: 'request2',
              productType: 'type2',
              senderPaId: 'sender2',
              tenderId: 'tender2',
              iun: 'iun2'
        },
        {
              kinesisSeqNumber: '1234567891',
              unifiedDeliveryDriver: 'driver3',
              recipientNormalizedAddress: { pr: 'address3', cap: '67890' },
              requestId: 'request3',
              productType: 'type3',
              senderPaId: 'sender3',
              tenderId: 'tender3',
              iun: 'iun3'
        }
      ],
    };

    const result = await lambda.handleEvent(event);
    expect(result.batchItemFailures).to.deep.equal(
      [ { itemIdentifier: '1234567890' } ] 
    );
  });

});
