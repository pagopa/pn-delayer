const { getItems, deleteItems, getPrintCapacity, updatePrintCapacityCounter, dynamoModule, getUsedPrintCapacities } = require("../../app/lib/dynamo");
const { DynamoDBDocumentClient, QueryCommand, BatchGetCommand} = require("@aws-sdk/lib-dynamodb");
const { DynamoDBClient, BatchWriteItemCommand } = require("@aws-sdk/client-dynamodb");

const sinon = require('sinon');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const { LocalDate, DayOfWeek, TemporalAdjusters } = require("@js-joda/core");
chai.use(chaiAsPromised);
const { expect } = chai;
    

describe("deleteItems", () => {
    let dynamoClientMock;

    beforeEach(() => {
        dynamoClientMock = sinon.stub();
        sinon.replace(DynamoDBClient.prototype, 'send', dynamoClientMock);
    });

    afterEach(() => {
        sinon.restore();
    });

    it("should delete items in batches", async () => {
        const requestIds = ["id1", "id2", "id3"];
        const deliveryDate = "2023-01-01";
    
        dynamoClientMock.onCall(0).resolves({ UnprocessedItems: {} });

        const result = await deleteItems(requestIds, deliveryDate);
        
        sinon.assert.callCount(dynamoClientMock, 1);
        sinon.assert.calledWith(dynamoClientMock, sinon.match.instanceOf(BatchWriteItemCommand));
        expect(result).to.deep.equal([]);
    });

    it("should delete items in batches with unprocessedItems", async () => {

        const requestIds = ["id1", "id2", "id3"];
        const deliveryDate = "2023-01-01";
        const deleteRequest = [
            { DeleteRequest: { Key: { deliveryDate, requestId: "id1" } } },
            { DeleteRequest: { Key: { deliveryDate, requestId: "id2" } } }
        ]
        
        dynamoClientMock.resolves({ UnprocessedItems: { [process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME]: deleteRequest } });

        const result = await deleteItems(requestIds, deliveryDate);
        
        sinon.assert.callCount(dynamoClientMock, 3);
        sinon.assert.calledWith(dynamoClientMock, sinon.match.instanceOf(BatchWriteItemCommand));
        expect(result).to.deep.equal(deleteRequest);
    });

    it("should delete items in batches with unprocessedItems only on first retry", async () => {

        const requestIds = ["id1", "id2", "id3"];
        const deliveryDate = "2023-01-01";
        const deleteRequest = [
            { DeleteRequest: { Key: { deliveryDate, requestId: "id1" } } },
            { DeleteRequest: { Key: { deliveryDate, requestId: "id2" } } }
        ]
        
        dynamoClientMock.onCall(0).resolves({ UnprocessedItems: { [process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME]: deleteRequest } })
        .onCall(1).resolves({ UnprocessedItems: {} });

        const result = await deleteItems(requestIds, deliveryDate);
        
        sinon.assert.callCount(dynamoClientMock, 2);
        sinon.assert.calledWith(dynamoClientMock, sinon.match.instanceOf(BatchWriteItemCommand));
        expect(result).to.deep.equal([]);
    });

    
    it("should delete items in batches with multiple chunk without unprocessedItems", async () => {

        const requestIds = Array.from({ length: 30 }, (_, i) => `id${i + 1}`);
        const deliveryDate = "2023-01-01";
        
        dynamoClientMock.resolves({ UnprocessedItems: {} });

        const result = await deleteItems(requestIds, deliveryDate);
        
        sinon.assert.callCount(dynamoClientMock, 2);
        sinon.assert.calledWith(dynamoClientMock, sinon.match.instanceOf(BatchWriteItemCommand));
        expect(result).to.deep.equal([]);
    });

    it("should delete items in batches with multiple chunk with unprocessedItems", async () => {

        const requestIds = Array.from({ length: 30 }, (_, i) => `id${i + 1}`);
        const deliveryDate = "2023-01-01";
        const deleteRequest1 = [
            { DeleteRequest: { Key: { deliveryDate, requestId: "id1" } } },
        ]
        const deleteRequest2 = [
            { DeleteRequest: { Key: { deliveryDate, requestId: "id26" } } }
        ]

        const deleteRequest3 = [
            { DeleteRequest: { Key: { deliveryDate, requestId: "id1" } } },
            { DeleteRequest: { Key: { deliveryDate, requestId: "id26" } } }
        ]

        dynamoClientMock.onCall(0).resolves({ UnprocessedItems: { [process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME]: deleteRequest1 } })
        .onCall(1).resolves({ UnprocessedItems: { [process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME]: deleteRequest2 } })
        .onCall(2).resolves({ UnprocessedItems: { [process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME]: deleteRequest3 } })
        .onCall(3).resolves({ UnprocessedItems: { [process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME]: deleteRequest3 } })

        const result = await deleteItems(requestIds, deliveryDate);
        
        sinon.assert.callCount(dynamoClientMock, 4);
        sinon.assert.calledWith(dynamoClientMock, sinon.match.instanceOf(BatchWriteItemCommand));
        expect(result).to.deep.equal(deleteRequest3);
    });

    it("should delete items in batches with multiple chunk with dynamo error", async () => {

        const requestIds = Array.from({ length: 30 }, (_, i) => `id${i + 1}`);
        const deliveryDate = "2023-01-01";
        const deleteRequest2 = [
            { DeleteRequest: { Key: { deliveryDate, requestId: "id26" } } },
            { DeleteRequest: { Key: { deliveryDate, requestId: "id27" } } },
            { DeleteRequest: { Key: { deliveryDate, requestId: "id28" } } },
            { DeleteRequest: { Key: { deliveryDate, requestId: "id29" } } },
            { DeleteRequest: { Key: { deliveryDate, requestId: "id30" } } }
        ]

        dynamoClientMock.onCall(0).resolves({ UnprocessedItems: {} })
        .onCall(1).rejects(new Error("DynamoDB error"))
        .onCall(2).resolves({ UnprocessedItems: { [process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME]: deleteRequest2 } })
        .onCall(3).resolves({ UnprocessedItems: { [process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME]: deleteRequest2 } })

        const result = await deleteItems(requestIds, deliveryDate);
        
        sinon.assert.callCount(dynamoClientMock, 4);
        sinon.assert.calledWith(dynamoClientMock, sinon.match.instanceOf(BatchWriteItemCommand));
        expect(result).to.deep.equal(deleteRequest2);
    });

   
});

describe("getItems", () => {
    let sendStub;

    beforeEach(() => {
        sendStub = sinon.stub();
        sinon.replace(DynamoDBDocumentClient.prototype, "send", sendStub);
        process.env.PAPER_DELIVERY_READYTOSEND_TABLENAME = "test-table";
    });

    afterEach(() => {
        sinon.restore();
    });

    it("should return result from docClient.send", async () => {
        const mockItems = [{ requestId: "1" }, { requestId: "2" }];
        const mockResult = { Items: mockItems, LastEvaluatedKey: { foo: "bar" } };
        sendStub.resolves(mockResult);

        const result = await getItems("priority1", "2024-06-01", undefined, 10);

        sinon.assert.calledOnce(sendStub);
        sinon.assert.calledWith(sendStub, sinon.match.instanceOf(QueryCommand));
        expect(result).to.deep.equal(mockResult);
    });

    it("should return default empty result if docClient.send returns undefined", async () => {
        sendStub.resolves(undefined);

        const result = await getItems("priority1", "2024-06-01", undefined, 10);

        sinon.assert.calledOnce(sendStub);
        expect(result).to.deep.equal({ Items: [], LastEvaluatedKey: {} });
    });

    it("should pass correct parameters to QueryCommand", async () => {
        sendStub.resolves({ Items: [], LastEvaluatedKey: {} });
        const priorityKey = "priorityX";
        const executionDate = "2024-06-02";
        const LastEvaluatedKey = { foo: "bar" };
        const limit = 5;

        const result = await getItems(priorityKey, executionDate, LastEvaluatedKey, limit);
        expect(result).to.deep.equal({ Items: [], LastEvaluatedKey: {} });
        
    });

    it("should parse limit as integer", async () => {
        sendStub.resolves({ Items: [] });
        await getItems("priority1", "2024-06-01", undefined, "7");
        const callArg = sendStub.firstCall.args[0];
        expect(callArg.input.Limit).to.equal(7);
    });
});

describe("getUsedPrintCapacities", () => {
        let sendStub;

        beforeEach(() => {
            sendStub = sinon.stub();
            sinon.replace(
                require("@aws-sdk/lib-dynamodb").DynamoDBDocumentClient.prototype,
                "send",
                sendStub
            );
            process.env.PAPER_DELIVERY_PRINTCAPACITYCOUNTER_TABLENAME = "counter-table";
            process.env.PN_DELAYER_DELIVERYDATEDAYOFWEEK = "1";
        });

        afterEach(() => {
            sinon.restore();
        });

        it("should return daily and weekly items if found", async () => {
            const today = LocalDate.now();
            const initialDayOfWeek = today.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(1))).toString();
            const items = [
                { pk: today.toString(), sk: "DAY", value: 10 },
                { pk: initialDayOfWeek, sk: "WEEK", value: 70 }
            ];
            const response = {
                Responses: {
                    "counter-table": items
                }
            };
            sendStub.resolves(response);

            // Act
            const result = await getUsedPrintCapacities();

            // Assert
            sinon.assert.calledOnce(sendStub);
            sinon.assert.calledWith(sendStub, sinon.match.instanceOf(BatchGetCommand));
            expect(result.daily).to.deep.equal(items[0]);
            expect(result.weekly).to.deep.equal(items[1]);
        });

        it("should return null for daily and weekly if not found", async () => {
            sendStub.resolves({ Responses: { "counter-table": [] } });
            const result = await getUsedPrintCapacities();
            expect(result.daily).to.be.null;
            expect(result.weekly).to.be.null;
        });

        it("should handle undefined Responses gracefully", async () => {
            sendStub.resolves({});
            const result = await getUsedPrintCapacities();
            expect(result.daily).to.be.null;
            expect(result.weekly).to.be.null;
        });
});

describe("getPrintCapacity", () => {
        let sendStub;

        beforeEach(() => {
            sendStub = sinon.stub();
            sinon.replace(
                require("@aws-sdk/lib-dynamodb").DynamoDBDocumentClient.prototype,
                "send",
                sendStub
            );
            process.env.PAPER_DELIVERY_PRINTCAPACITY_TABLENAME = "capacity-table";
        });

        afterEach(() => {
            sinon.restore();
        });

        it("should return Items from docClient.send", async () => {
            const items = [{ pk: "PRINT", value: 100 }];
            sendStub.resolves({ Items: items });
            const result = await getPrintCapacity();
            sinon.assert.calledOnce(sendStub);
            sinon.assert.calledWith(sendStub, sinon.match.instanceOf(QueryCommand));
            expect(result).to.deep.equal(items);
        });

        it("should return empty array if Items is undefined", async () => {
            sendStub.resolves({});
            const result = await getPrintCapacity();
            expect(result).to.deep.equal([]);
        });
});

describe("updatePrintCapacityCounter", () => {
    let sendStub;
    let UpdateCommand;

    beforeEach(() => {
        sendStub = sinon.stub();
        sinon.replace(
            require("@aws-sdk/lib-dynamodb").DynamoDBDocumentClient.prototype,
            "send",
            sendStub
        );
        UpdateCommand = require("@aws-sdk/lib-dynamodb").UpdateCommand;
        process.env.PAPER_DELIVERY_PRINTCAPACITYCOUNTER_TABLENAME = "counter-table";
    });

    afterEach(() => {
        sinon.restore();
    });

    it("should call docClient.send with correct UpdateCommand and params", async () => {
        sendStub.resolves();
        const date = "2024-06-03";
        const sk = "DAY";
        const increment = 5;

        await updatePrintCapacityCounter(date, sk, increment);

        sinon.assert.calledOnce(sendStub);
        const callArg = sendStub.firstCall.args[0];
        expect(callArg).to.be.instanceOf(UpdateCommand);
        expect(callArg.input).to.deep.include({
            TableName: "counter-table",
            Key: { pk: date, sk: sk },
            UpdateExpression: "ADD #counter :inc SET #ttl = :ttl",
            ExpressionAttributeNames: { "#counter": "counter", "#ttl": "ttl" },
            ExpressionAttributeValues: { ":inc": increment, ":ttl": callArg.input.ExpressionAttributeValues[":ttl"] }
        });
        expect(typeof callArg.input.ExpressionAttributeValues[":ttl"]).to.equal("number");
    });

    it("should throw and log error if docClient.send fails", async () => {
        const error = new Error("Dynamo error");
        sendStub.rejects(error);

        const consoleErrorStub = sinon.stub(console, "error");

        await chai.expect(
            updatePrintCapacityCounter("2024-06-03", "DAY", 1)
        ).to.be.rejectedWith("Dynamo error");

        sinon.assert.calledOnce(consoleErrorStub);
        expect(consoleErrorStub.firstCall.args[0]).to.include("Error incrementing print capacity counter:");
        consoleErrorStub.restore();
    });
 });

