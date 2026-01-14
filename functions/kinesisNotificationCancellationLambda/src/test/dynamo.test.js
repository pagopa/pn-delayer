const { expect } = require("chai");
const sinon = require("sinon");
const proxyquire = require("proxyquire");

describe("dynamo.js – cancellation logic", () => {
  let dynamo;
  let mockSend;
  let QueryCommand, TransactWriteCommand;

  beforeEach(() => {
    mockSend = sinon.stub();

    QueryCommand = function (params) {
      this.params = params;
    };

    TransactWriteCommand = function (params) {
      this.params = params;
    };

    const DynamoDBDocumentClient = {
      from: sinon.stub().returns({ send: mockSend })
    };

    dynamo = proxyquire("../app/lib/dynamo", {
      "@aws-sdk/client-dynamodb": {
        DynamoDBClient: function () {}
      },
      "@aws-sdk/lib-dynamodb": {
        QueryCommand,
        TransactWriteCommand,
        DynamoDBDocumentClient
      }
    });

    process.env.DELAYER_PAPER_DELIVERY_TABLE_NAME = "paperDeliveryTable";
  });

  afterEach(() => {
    sinon.restore();
  });

  describe("retrievePaperDelivery", () => {
    it("returns the most recent item when found", async () => {
      const item = { pk: "REQ1", sk: "2024-01-01" };

      mockSend.resolves({
        Items: [item]
      });

      const result = await dynamo.retrievePaperDelivery("REQ1");

      expect(result).to.deep.equal(item);
      expect(mockSend.calledOnce).to.be.true;

      const command = mockSend.firstCall.args[0];
      expect(command).to.be.instanceOf(QueryCommand);
      expect(command.params.IndexName).to.equal("requestId-CreatedAt-index");
      expect(command.params.Limit).to.equal(1);
      expect(command.params.ScanIndexForward).to.be.false;
    });

    it("returns null when no items are found", async () => {
      mockSend.resolves({ Items: [] });

      const result = await dynamo.retrievePaperDelivery("REQ_NOT_FOUND");

      expect(result).to.be.null;
    });

    it("propagates DynamoDB errors", async () => {
      mockSend.rejects(new Error("Query failed"));

      try {
        await dynamo.retrievePaperDelivery("REQ1");
        expect.fail("Should have thrown");
      } catch (err) {
        expect(err.message).to.equal("Query failed");
      }
    });
  });

  describe("executeTransactions", () => {
    it("returns success true when input is empty", async () => {
      const result = await dynamo.executeTransactions([], "seq-1");

      expect(result).to.deep.equal({
        success: true,
        kinesisSequenceNumber: "seq-1"
      });
      expect(mockSend.notCalled).to.be.true;
    });

    it("executes a transact write with delete + put", async () => {
      mockSend.resolves({});

      const items = [
        { pk: "PK1", sk: "SK1", attr: "value" }
      ];

      const result = await dynamo.executeTransactions(items, "seq-2");

      expect(result).to.deep.equal({
        success: true,
        kinesisSequenceNumber: "seq-2"
      });

      expect(mockSend.calledOnce).to.be.true;

      const command = mockSend.firstCall.args[0];
      expect(command).to.be.instanceOf(TransactWriteCommand);

      const actions = command.params.TransactItems;
      expect(actions).to.have.length(2);

      expect(actions[0]).to.have.property("Delete");
      expect(actions[1]).to.have.property("Put");
      expect(actions[1].Put.Item.pk).to.equal("DELETED~PK1");
    });

    it("returns success false when transaction fails", async () => {
      mockSend.rejects(new Error("Transaction failed"));

      const items = [{ pk: "PK2", sk: "SK2" }];

      const result = await dynamo.executeTransactions(items, "seq-3");

      expect(result).to.deep.equal({
        success: false,
        kinesisSequenceNumber: "seq-3"
      });
    });
  });
});
