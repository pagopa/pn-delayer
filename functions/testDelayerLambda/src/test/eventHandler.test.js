const assert = require('assert');
const { Readable } = require("stream");
const { handler } = require("../../index");
const fs = require("fs");
const path = require("path");

process.env.BUCKET_NAME = "test-bucket";
process.env.OBJECT_KEY = "test-key.csv";
process.env.SFN_ARN = "arn:aws:states:eu-south-1:123456789012:stateMachine:BatchWorkflowStateMachine";
process.env.DELAYERTOPAPERCHANNEL_SFN_ARN = "arn:aws:states:eu-south-1:123456789012:stateMachine:delayerToPaperChannelStateMachine";

const { mockClient } = require("aws-sdk-client-mock");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { DynamoDBDocumentClient, BatchWriteCommand, GetCommand, QueryCommand } = require("@aws-sdk/lib-dynamodb");
const { SFNClient, StartExecutionCommand } = require("@aws-sdk/client-sfn");

const s3Mock = mockClient(S3Client);
const ddbMock = mockClient(DynamoDBDocumentClient);
const sfnMock = mockClient(SFNClient);

describe("Lambda Delayer Dispatcher", () => {
    beforeEach(() => {
        s3Mock.reset();
        ddbMock.reset();
        sfnMock.reset();
    });

    it("Unsupported operation returns 400", async () => {

        const result = await handler({ operationType: "UNKNOWN_OP", parameters: [] });
        assert.strictEqual(result.statusCode, 400);
        assert.strictEqual(JSON.parse(result.body).message.includes("Unsupported operationType"), true);
    });

    it("should batch-write items to DynamoDB", async () => {
        const csvPath = path.join(__dirname, "sample.csv");
        const csvData = fs.readFileSync(csvPath, "utf8");
        s3Mock.on(GetObjectCommand).resolves({
            Body: Readable.from([csvData])
        });
        ddbMock.on(BatchWriteCommand).resolves({});

        const result = await handler({ operationType: "IMPORT_DATA", parameters: [] });
        assert.strictEqual(result.statusCode, 200);
        assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length > 0, true);
    });

    it("should batch-write items to DynamoDB with custom fileName", async () => {
        const csvPath = path.join(__dirname, "sample.csv");
        const csvData = fs.readFileSync(csvPath, "utf8");
        s3Mock.on(GetObjectCommand).resolves({
            Body: Readable.from([csvData])
        });
        ddbMock.on(BatchWriteCommand).resolves({});

        const result = await handler({ operationType: "IMPORT_DATA", parameters: ["fileName"] });
        assert.strictEqual(result.statusCode, 200);
        assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length > 0, true);
    });

    it("GET_USED_CAPACITY returns the item", async () => {
        const fakeItem = {
            unifiedDeliveryDriverGeokey: "Sailpost~87100",
            deliveryDate: "2025-06-30T00:00:00Z",
            geoKey: "87100",
            unifiedDeliveryDriver: "Sailpost",
            usedCapacity: 572,
            capacity: 1000,
        };
        ddbMock.on(GetCommand).resolves({ Item: fakeItem });
        const params = ["Sailpost", "87100", "2025-06-30T00:00:00Z"];

        const result = await handler({ operationType: "GET_USED_CAPACITY", parameters: params });
        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.usedCapacity, 572);
    });

    it("GET_USED_CAPACITY item not found", async () => {
        ddbMock.on(GetCommand).resolves({});
        const params = ["Nope", "00000", "2025-01-01T00:00:00Z"];

        const result = await handler({ operationType: "GET_USED_CAPACITY", parameters: params });
        assert.strictEqual(JSON.parse(result.body).message, "Item not found");
    });

    it("returns matching rows array", async () => {
        const rows = [
            { requestId: "RID123", pk: "a", createdAt: "2025-01-01T00:00:00Z" },
            { requestId: "RID123", pk: "b", createdAt: "2025-01-02T00:00:00Z" },
        ];
        ddbMock.on(QueryCommand).resolves({ Items: rows });

        const res = await handler({
            operationType: "GET_BY_REQUEST_ID",
            parameters: ["RID123"],
        });

        assert.strictEqual(res.statusCode, 200);
        assert.deepStrictEqual(JSON.parse(res.body), rows);
    });

    it("returns empty array when no items", async () => {
        ddbMock.on(QueryCommand).resolves({ Items: [] });

        const res = await handler({
            operationType: "GET_BY_REQUEST_ID",
            parameters: ["NOT_EXISTS"],
        });

        assert.strictEqual(res.statusCode, 200);
        assert.deepStrictEqual(JSON.parse(res.body), []);

    });

    it("error when no requestId provided", async () => {
        ddbMock.on(QueryCommand).resolves({ Items: [] });

        const res = await handler({
            operationType: "GET_BY_REQUEST_ID",
            parameters: [],
        });

        assert.strictEqual(res.statusCode, 500);
    });

    it("starts the step function and returns executionArn", async () => {
        const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec123";
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({ 
            executionArn: fakeArn, 
            startDate: fakeStartDate 
        });

        const printCapacity = "180000";
        const deliveryDay = "1";

        const result = await handler({ operationType: "RUN_ALGORITHM", parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities",
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit","pn-PaperDeliveryPrintCapacity",
                "pn-PaperDeliveryCounters", printCapacity, deliveryDay] });

        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.executionArn, fakeArn);
        
        const calls = sfnMock.commandCalls(StartExecutionCommand);
        assert.strictEqual(calls.length, 1);
        
        const input = JSON.parse(calls[0].args[0].input.input);
        assert.strictEqual(input.PN_DELAYER_PRINTCAPACITY, `1970-01-01;${printCapacity}`);
        assert.strictEqual(input.PN_DELAYER_DELIVERYDATEDAYOFWEEK, deliveryDay);
    });

      it("starts the step function with default parameters when none are provided", async () => {
        const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec456";
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({ 
            executionArn: fakeArn, 
            startDate: fakeStartDate 
        });

        const result = await handler({ operationType: "RUN_ALGORITHM", parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities",
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit","pn-PaperDeliveryPrintCapacity",
                "pn-PaperDeliveryCounters"] });

        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.executionArn, fakeArn);
        
        const calls = sfnMock.commandCalls(StartExecutionCommand);
        assert.strictEqual(calls.length, 1);
        
        const input = JSON.parse(calls[0].args[0].input.input);
        assert.strictEqual(input.PN_DELAYER_PRINTCAPACITY, "1970-01-01;180000"); //default
        assert.strictEqual(input.PN_DELAYER_DELIVERYDATEDAYOFWEEK, "1"); //default
    });

    it("starts the step function with partial optional parameters (only printCapacity)", async () => {
        const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec789";
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({ 
            executionArn: fakeArn, 
            startDate: fakeStartDate 
        });

        const printCapacity = "180000";

        const result = await handler({operationType: "RUN_ALGORITHM",parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities",
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit","pn-PaperDeliveryPrintCapacity",
                "pn-PaperDeliveryCounters", printCapacity] });

        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.executionArn, fakeArn);
        
        const calls = sfnMock.commandCalls(StartExecutionCommand);
        assert.strictEqual(calls.length, 1);
        
        const input = JSON.parse(calls[0].args[0].input.input);
        assert.strictEqual(input.PN_DELAYER_PRINTCAPACITY, `1970-01-01;${printCapacity}`);
        assert.strictEqual(input.PN_DELAYER_DELIVERYDATEDAYOFWEEK, "1"); // default
    });

    it("error the step function with no required parameters provided", async () => {
        const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec456";
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({
            executionArn: fakeArn,
            startDate: fakeStartDate
        });

        const result = await handler({ operationType: "RUN_ALGORITHM", parameters: ["pn-DelayerPaperDelivery"] });

        assert.strictEqual(result.statusCode, 500);
    });

        it("starts the step function and returns executionArn", async () => {
        const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec123";
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({ executionArn: fakeArn, startDate: fakeStartDate });

         const deliveryDay = "1";

        const result = await handler({ operationType: "DELAYER_TO_PAPER_CHANNEL", parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryCounters", deliveryDay] });
        assert.strictEqual(result.statusCode, 200);
    
        const body = JSON.parse(result.body);
        assert.strictEqual(body.executionArn, fakeArn);
        assert.ok(body.startDate);
    
        const calls = sfnMock.commandCalls(StartExecutionCommand);
        assert.strictEqual(calls.length, 1);
        
        const input = JSON.parse(calls[0].args[0].input.input);
        assert.strictEqual(input.PN_DELAYER_DELIVERYDATEDAYOFWEEK, "1");

    });

    it("should use default deliveryDateDayOfWeek if not provided", async () => {
        const fakeArn = "arn:aws:states:...:execution:delayerToPaperChannelStateMachine:exec123";
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({ executionArn: fakeArn, startDate: fakeStartDate });

        const result = await handler({ operationType: "DELAYER_TO_PAPER_CHANNEL", parameters: ["pn-DelayerPaperDelivery","pn-PaperDeliveryCounters"] });

        const body = JSON.parse(result.body);
        assert.strictEqual(body.executionArn, fakeArn);
        assert.ok(body.startDate);

        const calls = sfnMock.commandCalls(StartExecutionCommand);
        assert.strictEqual(calls.length, 1);
        
        const input = JSON.parse(calls[0].args[0].input.input);
        assert.strictEqual(input.PN_DELAYER_DELIVERYDATEDAYOFWEEK, "1");
    });

    it("should throw if DELAYERTOPAPERCHANNEL_SFN_ARN is missing", async () => {
        delete process.env.DELAYERTOPAPERCHANNEL_SFN_ARN;
        try {
            await handler({ operationType: "DELAYER_TO_PAPER_CHANNEL", parameters: [] });
            throw new Error("Should have thrown");
        } catch (err) {
            return err.message.includes("Missing environment variable DELAYERTOPAPERCHANNEL_SFN_ARN");
        }
    });

   it("DELETE_DATA delete batch record DynamoDB", async () => {
   const csvPath = path.join(__dirname, "sample.csv");
       const csvData = fs.readFileSync(csvPath, "utf8");
       s3Mock.on(GetObjectCommand).resolves({
           Body: Readable.from([csvData])
       });
       ddbMock.on(QueryCommand).resolves({ Items: [{ pk: "pk1", sk: "sk1", province: "RM", productType: "RS", senderPaId: "PaId" }] });
       ddbMock.on(BatchWriteCommand).resolves({});
       ddbMock.on(BatchWriteCommand).resolves({});

       const result = await handler({ operationType: "DELETE_DATA", parameters: [] });
       assert.strictEqual(result.statusCode, 200);
       const body = JSON.parse(result.body);
       assert.strictEqual(body.message, "Delete completed");
       assert.strictEqual(typeof body.processed, "number");
   });

   it("DELETE_DATA with custom fileName", async () => {
       const csvPath = path.join(__dirname, "sample.csv");
              const csvData = fs.readFileSync(csvPath, "utf8");
              s3Mock.on(GetObjectCommand).resolves({
                  Body: Readable.from([csvData])
              });
       ddbMock.on(QueryCommand).resolves({ Items: [{ pk: "pk2", sk: "sk2", province: "MI", productType: "RS", senderPaId: "PaId" }] });
       ddbMock.on(BatchWriteCommand).resolves({});

       const result = await handler({ operationType: "DELETE_DATA", parameters: ["custom.csv"] });
       assert.strictEqual(result.statusCode, 200);
       const body = JSON.parse(result.body);
       assert.strictEqual(body.message, "Delete completed");
   });
});