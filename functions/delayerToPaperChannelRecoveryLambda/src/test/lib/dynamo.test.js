const { getItemsChunk, deleteItems } = require("../../app/lib/dynamo");
const { DynamoDBDocumentClient, QueryCommand} = require("@aws-sdk/lib-dynamodb");
const { DynamoDBClient, BatchWriteItemCommand } = require("@aws-sdk/client-dynamodb");

const sinon = require('sinon');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const { expect } = chai;
    
describe("getItemsChunk", () => {
    let dynamoDocumentClientMock;
    
    beforeEach(() => {
        dynamoDocumentClientMock = sinon.stub();
        sinon.replace(DynamoDBDocumentClient.prototype, 'send', dynamoDocumentClientMock);
    });

    afterEach(() => {
        sinon.restore();
    });

    it("should return items from the table with null lastEvaluatedKey", async () => {
        const deliveryDate = "2023-01-01";
        const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
        const lastEvaluatedKey = null;
        dynamoDocumentClientMock.onCall(0).resolves({ Items: items, LastEvaluatedKey: lastEvaluatedKey });

        const result = await getItemsChunk(deliveryDate);

        sinon.assert.callCount(dynamoDocumentClientMock, 1);
        sinon.assert.calledWith(dynamoDocumentClientMock, sinon.match.instanceOf(QueryCommand));
        expect(result).to.deep.equal({items, lastKey: lastEvaluatedKey});
    
    });

    it("should return items from the table with notNull lastEvaluatedKey", async () => {
        const deliveryDate = "2023-01-01";
        const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
        const lastEvaluatedKey = {requestId: {S:'id1'}, deliveryDate: {S:'2023-01-01T00:00:00.000Z'}};
        dynamoDocumentClientMock.onCall(0).resolves({ Items: items, LastEvaluatedKey: lastEvaluatedKey });

        const result = await getItemsChunk(deliveryDate);

        sinon.assert.callCount(dynamoDocumentClientMock, 1);
        sinon.assert.calledWith(dynamoDocumentClientMock, sinon.match.instanceOf(QueryCommand));
        expect(result).to.deep.equal({items, lastKey: lastEvaluatedKey});
    
    });

    it("should return items from the table with notNull lastEvaluatedKey input", async () => {
        const deliveryDate = "2023-01-01";
        const items = [{ requestId: {S:'1' }, iun: {S:'iun1'}}, { requestId: {S:'2' }, iun: {S:'iun2'}}];
        const lastEvaluatedKey = {requestId: {S:'id1'}, deliveryDate: {S:'2023-01-01T00:00:00.000Z'}};
        dynamoDocumentClientMock.onCall(0).resolves({ Items: items, LastEvaluatedKey: null });

        const result = await getItemsChunk(deliveryDate, lastEvaluatedKey);

        sinon.assert.callCount(dynamoDocumentClientMock, 1);
        sinon.assert.calledWith(dynamoDocumentClientMock, sinon.match.instanceOf(QueryCommand));
        expect(result).to.deep.equal({items, lastKey: null});
    
    });

    it("should return an empty array if no items are found", async () => {
        const deliveryDate = "2023-01-01";
        dynamoDocumentClientMock.onCall(0).resolves({ Items: [] , LastEvaluatedKey: null });

        const result = await getItemsChunk(deliveryDate);

        sinon.assert.callCount(dynamoDocumentClientMock, 1);
        sinon.assert.calledWith(dynamoDocumentClientMock, sinon.match.instanceOf(QueryCommand));
        expect(result).to.deep.equal({items: [], lastKey: null});
    });
});

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
        
        dynamoClientMock.resolves({ UnprocessedItems: { [process.env.READY_TO_SEND_TABLE_NAME]: deleteRequest } });

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
        
        dynamoClientMock.onCall(0).resolves({ UnprocessedItems: { [process.env.READY_TO_SEND_TABLE_NAME]: deleteRequest } })
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

        dynamoClientMock.onCall(0).resolves({ UnprocessedItems: { [process.env.READY_TO_SEND_TABLE_NAME]: deleteRequest1 } })
        .onCall(1).resolves({ UnprocessedItems: { [process.env.READY_TO_SEND_TABLE_NAME]: deleteRequest2 } })
        .onCall(2).resolves({ UnprocessedItems: { [process.env.READY_TO_SEND_TABLE_NAME]: deleteRequest3 } })
        .onCall(3).resolves({ UnprocessedItems: { [process.env.READY_TO_SEND_TABLE_NAME]: deleteRequest3 } }) 

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
        .onCall(2).resolves({ UnprocessedItems: { [process.env.READY_TO_SEND_TABLE_NAME]: deleteRequest2 } })
        .onCall(3).resolves({ UnprocessedItems: { [process.env.READY_TO_SEND_TABLE_NAME]: deleteRequest2 } })

        const result = await deleteItems(requestIds, deliveryDate);
        
        sinon.assert.callCount(dynamoClientMock, 4);
        sinon.assert.calledWith(dynamoClientMock, sinon.match.instanceOf(BatchWriteItemCommand));
        expect(result).to.deep.equal(deleteRequest2);
    });

   
});
