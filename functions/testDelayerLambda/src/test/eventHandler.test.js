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

        const result = await handler({ operationType: "IMPORT_DATA", parameters: ["pn-DelayerPaperDelivery", "pn-PaperDeliveryCounters"] });
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

        const result = await handler({ operationType: "IMPORT_DATA", parameters: ["pn-DelayerPaperDelivery","pn-PaperDeliveryCounters", "fileName"] });
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
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit",
                "pn-PaperDeliveryCounters", printCapacity] });

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
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit",
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
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit",
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
                "pn-PaperDeliveryCounters"] });
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
       ddbMock.on(QueryCommand).resolves({ Items: [{ pk: "2025-08-25~EVALUATE_PRINT_CAPACITY", sk: "sk1", province: "RM", productType: "RS", senderPaId: "PaId", 
        unifiedDeliveryDriver: "driver1", cap: "00178" },{ pk: "2025-08-25~EVALUATE_PRINT_CAPACITY", sk: "sk2", province: "RM", productType: "RS", senderPaId: "PaId", 
        unifiedDeliveryDriver: "driver1", cap: "00179" },{ pk: "2025-08-25~EVALUATE_PRINT_CAPACITY", sk: "sk3", province: "NA", productType: "RS", senderPaId: "PaId", 
        unifiedDeliveryDriver: "driver1", cap: "20100" }] });
       ddbMock.on(BatchWriteCommand).resolves({});
       ddbMock.on(BatchWriteCommand).resolves({});

       const result = await handler({ operationType: "DELETE_DATA", parameters: ["pn-DelayerPaperDelivery","pn-PaperDeliveryDriverUsedCapacities", "pn-PaperDeliveryUsedSenderLimit", "pn-PaperDeliveryCounters"] });
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
        ddbMock.on(QueryCommand).resolves({ Items: [{ pk: "2025-08-25~EVALUATE_PRINT_CAPACITY", sk: "sk1", province: "RM", productType: "RS", senderPaId: "PaId", 
        unifiedDeliveryDriver: "driver1", cap: "00178" },{ pk: "2025-08-25~EVALUATE_PRINT_CAPACITY", sk: "sk2", province: "RM", productType: "RS", senderPaId: "PaId", 
        unifiedDeliveryDriver: "driver1", cap: "00179" },{ pk: "2025-08-25~EVALUATE_PRINT_CAPACITY", sk: "sk3", province: "NA", productType: "RS", senderPaId: "PaId", 
        unifiedDeliveryDriver: "driver1", cap: "20100" }] });
       ddbMock.on(BatchWriteCommand).resolves({});

       const result = await handler({ operationType: "DELETE_DATA", parameters: ["pn-DelayerPaperDelivery",
       "pn-PaperDeliveryDriverUsedCapacities", "pn-PaperDeliveryUsedSenderLimit", "pn-PaperDeliveryCounters", "curstom.csv"] });
       assert.strictEqual(result.statusCode, 200);
       const body = JSON.parse(result.body);
       assert.strictEqual(body.message, "Delete completed");
   });

   it("GET_DECLARED_CAPACITY returns filtered items based on date range", async () => {
       const fakeItems = [
           {
               pk: "tender1~driver1~geo1",
               activationDateFrom: "2024-01-01T00:00:00Z",
               activationDateTo: "2024-12-31T00:00:00Z",
               capacity: 1000
           },
           {
               pk: "tender1~driver1~geo1",
               activationDateFrom: "2025-06-01T00:00:00Z",
               activationDateTo: "2025-06-30T00:00:00Z",
               capacity: 500
           },
           {
               pk: "tender1~driver1~geo1",
               activationDateFrom: "2025-01-01T00:00:00Z",
               activationDateTo: null, // No end date
               capacity: 2000
           }
       ];

       ddbMock.on(QueryCommand).resolves({ Items: fakeItems });
       const params = ["tender1", "driver1", "geo1", "2025-06-15T00:00:00Z"];

       const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });

       assert.strictEqual(result.statusCode, 200);
       const body = JSON.parse(result.body);
       // Should return 2 items: the one with no end date and the one covering June
       assert.strictEqual(body.length, 2);
       assert.strictEqual(body.some(item => item.capacity === 500), true);
       assert.strictEqual(body.some(item => item.capacity === 2000), true);
   });

   it("GET_DECLARED_CAPACITY returns empty array when no items match date range", async () => {
       const fakeItems = [
           {
               pk: "tender1~driver1~geo1",
               activationDateFrom: "2025-01-01T00:00:00Z",
               activationDateTo: "2025-05-31T00:00:00Z",
               capacity: 1000
           }
       ];

       ddbMock.on(QueryCommand).resolves({ Items: fakeItems });
       const params = ["tender1", "driver1", "geo1", "2025-06-15T00:00:00Z"];

       const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });

       assert.strictEqual(result.statusCode, 200);
       const body = JSON.parse(result.body);
       assert.strictEqual(body.length, 0);
   });

   it("GET_DECLARED_CAPACITY returns empty array when no items found in DynamoDB", async () => {
       ddbMock.on(QueryCommand).resolves({ Items: [] });
       const params = ["tender1", "driver1", "geo1", "2025-06-15T00:00:00Z"];

       const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });

       assert.strictEqual(result.statusCode, 200);
       const body = JSON.parse(result.body);
       assert.strictEqual(body.length, 0);
   });

   it("GET_DECLARED_CAPACITY error when missing required parameters", async () => {
       const params = [];

       const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });

       assert.strictEqual(result.statusCode, 500);
       const body = JSON.parse(result.body);
       assert.strictEqual(body.message.includes("Parameters must be"), true);
   });

   it("GET_DECLARED_CAPACITY error when empty parameters array", async () => {
       const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: [] });

       assert.strictEqual(result.statusCode, 500);
       const body = JSON.parse(result.body);
       assert.strictEqual(JSON.parse(result.body).message, "Parameters must be [tenderId, unifiedDeliveryDriver, geoKey, deliveryDate]");
   });
});