const assert = require('assert');
const { Readable } = require("stream");
const fs = require("fs");
const path = require("path");
const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");
const { LocalDate } = require("@js-joda/core");

process.env.AWS_REGION = "eu-south-1";
process.env.BUCKET_NAME = "test-bucket";
process.env.OBJECT_KEY = "test-key.csv";
process.env.ATHENA_DATABASE_NAME = "test-db";
process.env.ATHENA_WORKGROUP_NAME = "test-wg";
process.env.SFN_ARN = "arn:aws:states:eu-south-1:123456789012:stateMachine:BatchWorkflowStateMachine";
process.env.DELAYERTOPAPERCHANNEL_SFN_ARN = "arn:aws:states:eu-south-1:123456789012:stateMachine:delayerToPaperChannelStateMachine";
process.env.DELAYERTOPAPERCHANNELFIRSTSCHEDULERCRON = "cron(0 8 ? * MON-FRI *)";
process.env.DELAYERTOPAPERCHANNELSECONDSCHEDULERCRON = "cron(0 12 ? * MON-FRI *)";
process.env.DELAYERTOPAPERCHANNELFIRSTSCHEDULERSTARTDATE = "2025-07-01T08:00:00.000Z";
process.env.DELAYERTOPAPERCHANNELSECONDSCHEDULERSTARTDATE = "2025-07-01T08:00:00.000Z";

const { mockClient } = require("aws-sdk-client-mock");
const { S3Client, GetObjectCommand , CopyObjectCommand, PutObjectCommand, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } = require("@aws-sdk/client-athena");
const { DynamoDBDocumentClient, BatchWriteCommand, GetCommand, QueryCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const { SFNClient, StartExecutionCommand, DescribeExecutionCommand, ListExecutionsCommand } = require("@aws-sdk/client-sfn");

const s3Mock = mockClient(S3Client);
const ddbMock = mockClient(DynamoDBDocumentClient);
const sfnMock = mockClient(SFNClient);
const lambdaMock = mockClient(LambdaClient);
const athenaMock = mockClient(AthenaClient);

const proxyquire = require("proxyquire").noCallThru();

let handler;

describe("Lambda Delayer Dispatcher", () => {
    beforeEach(() => {
        s3Mock.reset();
        ddbMock.reset();
        sfnMock.reset();
        lambdaMock.reset();
        athenaMock.reset();

        ({ handler } = proxyquire("../../index", {
            "./src/app/lib/s3": {
                convertAthenaCsvToSemicolonCsv: async () => ({
                    bucket: "test-bucket",
                    key: "athena-results/residual-papers/test.csv",
                }),
                generatePresignedDownloadUrl: async () =>
                    "https://fake-presigned-url.s3.amazonaws.com/test.csv",
            },
        }));
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

        const result = await handler({ operationType: "IMPORT_DATA", parameters: ["pn-DelayerPaperDelivery", "pn-PaperDeliveryCounters", "pn-PaperDeliverySenderLimit", "file.csv"] });
        assert.strictEqual(result.statusCode, 200);
        assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length > 0, true);
        assert.strictEqual(ddbMock.commandCalls(UpdateCommand).length, 2);
    });

    it("should batch-write items to DynamoDB with deliveryWeekInput", async () => {
        const csvPath = path.join(__dirname, "sample.csv");
        const csvData = fs.readFileSync(csvPath, "utf8");
        s3Mock.on(GetObjectCommand).resolves({
            Body: Readable.from([csvData])
        });
        ddbMock.on(BatchWriteCommand).resolves({});

        const result = await handler({ operationType: "IMPORT_DATA", parameters: ["pn-DelayerPaperDelivery", "pn-PaperDeliveryCounters", "pn-PaperDeliverySenderLimit", "180000", "2025-08-04"] });
        assert.strictEqual(result.statusCode, 200);
        assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length > 0, true);
        assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand)[0].args[0].input.RequestItems["pn-DelayerPaperDelivery"][0].PutRequest.Item.pk, "2025-08-04~EVALUATE_SENDER_LIMIT");
        assert.strictEqual(ddbMock.commandCalls(UpdateCommand).length, 2);
    });

    it("should batch-write items to DynamoDB with custom fileName", async () => {
        const csvPath = path.join(__dirname, "sample.csv");
        const csvData = fs.readFileSync(csvPath, "utf8");
        s3Mock.on(GetObjectCommand).resolves({
            Body: Readable.from([csvData])
        });
        ddbMock.on(BatchWriteCommand).resolves({});

        const result = await handler({ operationType: "IMPORT_DATA", parameters: ["pn-DelayerPaperDelivery", "pn-PaperDeliveryCounters", "pn-PaperDeliverySenderLimit", "fileName"] });
        assert.strictEqual(result.statusCode, 200);
        assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length > 0, true);
        assert.strictEqual(ddbMock.commandCalls(UpdateCommand).length, 2);
    });

    function buildImportCsv(rowsCount) {
        const header = "requestId;notificationSentAt;prepareRequestDate;productType;senderPaId;province;cap;attempt;iun;senderPriority";
        const rows = Array.from({ length: rowsCount }, (_, i) =>
            `RID-${i};1970-01-05T00:00:00Z;1970-01-05T00:00:00Z;AR;sender-${i % 3};RM;00100;0;IUN-${i};10`
        );
        return [header, ...rows].join("\n");
    }

    it("IMPORT_DATA batchWriteItems must split payload in chunks of max 25", async () => {
        const csvData = buildImportCsv(60);

        s3Mock.on(GetObjectCommand).resolves({
            Body: Readable.from([csvData])
        });

        // Nessun sender limit valido -> i record delayed passano comunque nel flusso comune
        ddbMock.on(GetCommand).resolves({});
        ddbMock.on(BatchWriteCommand).resolves({});

        const result = await handler({
            operationType: "IMPORT_DATA",
            parameters: [
                "pn-DelayerPaperDelivery",
                "pn-PaperDeliveryCounters",
                "pn-PaperDeliverySenderLimit",
                "large.csv"
            ]
        });

        assert.strictEqual(result.statusCode, 200);

        const calls = ddbMock.commandCalls(BatchWriteCommand);
        assert.strictEqual(calls.length, 3);

        const chunkSizes = calls.map(
            (c) => c.args[0].input.RequestItems["pn-DelayerPaperDelivery"].length
        );

        assert.deepStrictEqual(chunkSizes, [25, 25, 10]);
        assert.strictEqual(chunkSizes.every((n) => n <= 25), true);
    });

    it("IMPORT_DATA batchWriteItems must retry UnprocessedItems and continue with remaining records", async () => {
        const csvData = buildImportCsv(30);

        s3Mock.on(GetObjectCommand).resolves({
            Body: Readable.from([csvData])
        });

        ddbMock.on(GetCommand).resolves({});

        let invocation = 0;
        ddbMock.on(BatchWriteCommand).callsFake(async (input) => {
            invocation += 1;

            const req = input.RequestItems["pn-DelayerPaperDelivery"];

            // Prima chiamata: simula 3 record non processati
            if (invocation === 1) {
                return {
                    UnprocessedItems: {
                        "pn-DelayerPaperDelivery": req.slice(0, 3)
                    }
                };
            }

            // Chiamate successive: tutto ok
            return {};
        });

        const result = await handler({
            operationType: "IMPORT_DATA",
            parameters: [
                "pn-DelayerPaperDelivery",
                "pn-PaperDeliveryCounters",
                "pn-PaperDeliverySenderLimit",
                "retry.csv"
            ]
        });

        assert.strictEqual(result.statusCode, 200);

        const calls = ddbMock.commandCalls(BatchWriteCommand);
        assert.strictEqual(calls.length, 2);

        const firstCallItems = calls[0].args[0].input.RequestItems["pn-DelayerPaperDelivery"];
        const secondCallItems = calls[1].args[0].input.RequestItems["pn-DelayerPaperDelivery"];

        assert.strictEqual(firstCallItems.length, 25);
        assert.strictEqual(secondCallItems.length, 8); // 5 rimanenti + 3 retry
        assert.strictEqual(firstCallItems.length <= 25, true);
        assert.strictEqual(secondCallItems.length <= 25, true);

        const retriedIds = firstCallItems.slice(0, 3).map((x) => x.PutRequest.Item.requestId);
        const secondCallIds = secondCallItems.map((x) => x.PutRequest.Item.requestId);

        retriedIds.forEach((id) => {
            assert.strictEqual(secondCallIds.includes(id), true);
        });

        const uniqueIds = new Set(
            calls.flatMap((c) =>
                c.args[0].input.RequestItems["pn-DelayerPaperDelivery"].map(
                    (x) => x.PutRequest.Item.requestId
                )
            )
        );

        assert.strictEqual(uniqueIds.size, 30);
    });

    it("IMPORT_DATA should handle mixed current and delayed records with valid sender limit", async () => {
    	LocalDate.now = () => LocalDate.parse("1970-01-07"); // mercoledi', currentWeek = 1970-01-05 (lunedi')
        // CSV con date miste: alcuni record current week (1970-01-05), altri delayed (1970-01-01)
        const csvData = [
            "requestId;notificationSentAt;prepareRequestDate;productType;senderPaId;province;cap;attempt;iun;senderPriority",
            "RID-CURRENT-1;1970-01-05T00:00:00Z;1970-01-05T00:00:00Z;AR;sender1;RM;00100;0;IUN-1;10",
            "RID-CURRENT-2;1970-01-05T00:00:00Z;1970-01-05T00:00:00Z;AR;sender1;RM;00100;0;IUN-2;20",
            "RID-DELAYED-1;1970-01-01T00:00:00Z;1970-01-01T00:00:00Z;AR;sender1;RM;00100;0;IUN-3;15",
            "RID-DELAYED-2;1970-01-01T00:00:00Z;1970-01-01T00:00:00Z;RS;sender2;NA;80124;0;IUN-4;30",
            "RID-CURRENT-3;1970-01-05T00:00:00Z;1970-01-05T00:00:00Z;890;sender3;MI;20100;0;IUN-5;25"
        ].join("\n");

        s3Mock.on(GetObjectCommand).resolves({
            Body: Readable.from([csvData])
        });

        // Mock getSenderLimit: ritorna weeklyEstimate > 0 per delayed records
        ddbMock.on(GetCommand).resolves({
            Item: {
                weeklyEstimate: 100
            }
        });

        // Mock batchWriteCommand e updateCommand
        ddbMock.on(BatchWriteCommand).resolves({});
        ddbMock.on(UpdateCommand).resolves({});

        const result = await handler({
            operationType: "IMPORT_DATA",
            parameters: [
                "pn-DelayerPaperDelivery",
                "pn-PaperDeliveryCounters",
                "pn-PaperDeliverySenderLimit",
                "mixed.csv"
            ]
        });

        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.processed, 5);

        // Verifica che BatchWriteCommand sia stato chiamato
        const batchCalls = ddbMock.commandCalls(BatchWriteCommand);
        assert.strictEqual(batchCalls.length, 1);

        // Raccogli tutti gli item scritti in DynamoDB
        const allWrittenItems = batchCalls.flatMap((call) =>
            call.args[0].input.RequestItems["pn-DelayerPaperDelivery"].map(
                (req) => req.PutRequest.Item
            )
        );

        // Verifica che tutti i 5 record siano stati elaborati
        assert.strictEqual(allWrittenItems.length, 5);

        // Verifica record current week (delayed = false)
        const currentRecords = allWrittenItems.filter((r) =>
            r.requestId.includes("CURRENT")
        );
        assert.strictEqual(currentRecords.length, 3);
        currentRecords.forEach((r) => {
            assert.strictEqual(r.delayed, false);
            assert.strictEqual(r.workflowStep, "EVALUATE_SENDER_LIMIT");
        });

        // Verifica record delayed che hanno sender limit valido (delayed = true)
        const delayedWithSenderLimit = allWrittenItems.filter((r) =>
            r.requestId === "RID-DELAYED-1"
        );
        assert.strictEqual(delayedWithSenderLimit.length, 1);
        assert.strictEqual(delayedWithSenderLimit[0].delayed, true);
        assert.strictEqual(delayedWithSenderLimit[0].skipSenderLimit, false);
        assert.strictEqual(delayedWithSenderLimit[0].senderPaId, "sender1");
        assert.strictEqual(delayedWithSenderLimit[0].province, "RM");

        // Verifica record delayed RS
        const delayedRS = allWrittenItems.filter((r) =>
            r.requestId === "RID-DELAYED-2"
        );
        assert.strictEqual(delayedRS.length, 1);
        assert.strictEqual(delayedRS[0].productType, "RS");
        assert.strictEqual(delayedRS[0].senderPaId, "sender2");
        assert.strictEqual(delayedRS[0].delayed, true);
        assert.strictEqual(delayedRS[0].skipSenderLimit, true);

        const updateCalls = ddbMock.commandCalls(UpdateCommand);
        assert.strictEqual(updateCalls.length, 6);

        const delayedCounterUpdates = updateCalls.filter((call) => {
            const sk = call.args[0].input.Key.sk;
            return sk && sk.startsWith("DELAYED~");
        });

        assert.strictEqual(delayedCounterUpdates.length, 2);

        // Verifica i parametri di updateDelayedCounter
        delayedCounterUpdates.forEach((call) => {
            const input = call.args[0].input;
            const sk = input.Key.sk;

            assert.strictEqual(sk.includes("DELAYED~"), true);
            assert.strictEqual(input.ExpressionAttributeValues[":numberOfShipments"], 1);
            assert.strictEqual(input.ExpressionAttributeValues[":weeklyEstimate"], 100);
        });
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
        const params = ["pn-PaperDeliveryDriverUsedCapacities", "Sailpost", "87100", "2025-06-30T00:00:00Z"];

        const result = await handler({ operationType: "GET_USED_CAPACITY", parameters: params });
        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.usedCapacity, 572);
    });

    it("GET_USED_CAPACITY item not found", async () => {
        ddbMock.on(GetCommand).resolves({});
        const params = ["pn-PaperDeliveryDriverUsedCapacities", "Nope", "00000", "2025-01-01T00:00:00Z"];

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
        sfnMock.on(ListExecutionsCommand).resolves({
            executions: []
        });

        const printCapacity = "180000";
        const deliveryDay = "1";

        const result = await handler({ operationType: "RUN_ALGORITHM", parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities",
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit",
                "pn-PaperDeliveryCounters", printCapacity] });

        assert.strictEqual(result.statusCode, 200);
        const outerBodyParse = JSON.parse(result.body);
        const body = JSON.parse(outerBodyParse.body);
        assert.strictEqual(body.executionArn, fakeArn);
        
        const calls = sfnMock.commandCalls(StartExecutionCommand);
        assert.strictEqual(calls.length, 1);
        
        const input = JSON.parse(calls[0].args[0].input.input);
        assert.strictEqual(input.PN_DELAYER_PRINTCAPACITY, `1970-01-01;${printCapacity}`);
        assert.strictEqual(input.PN_DELAYER_DELIVERYDATEDAYOFWEEK, deliveryDay);
    });

    it("starts the step function while there is another one executing", async () => {
        const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec123";
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({
            executionArn: fakeArn,
            startDate: fakeStartDate,
        });
        sfnMock.on(ListExecutionsCommand).resolves({
            executions: [{ executionArn: fakeArn,
            status: "RUNNING"
             }]
        });

        const printCapacity = "180000";
        const deliveryDay = "1";

        const result = await handler({ operationType: "RUN_ALGORITHM", parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities",
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit",
                "pn-PaperDeliveryCounters", printCapacity] });

        assert.strictEqual(result.statusCode, 200);
        const outerBodyParse = JSON.parse(result.body);
        const body = JSON.parse(outerBodyParse.body);
        assert.strictEqual(body.message, "There is already an active execution of the Step Function");
        assert.strictEqual(body.executionArn, fakeArn);
    });

      it("starts the step function with default parameters when none are provided", async () => {
        const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec456";
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({ 
            executionArn: fakeArn, 
            startDate: fakeStartDate 
        });
        sfnMock.on(ListExecutionsCommand).resolves({
            executions: []
        });

        const result = await handler({ operationType: "RUN_ALGORITHM", parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities",
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit",
                "pn-PaperDeliveryCounters", "180000"] });

        assert.strictEqual(result.statusCode, 200);
        const outerBodyParse = JSON.parse(result.body);
        const body = JSON.parse(outerBodyParse.body);
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
        sfnMock.on(ListExecutionsCommand).resolves({
            executions: []
        });

        const printCapacity = "180000";

        const result = await handler({operationType: "RUN_ALGORITHM",parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities",
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit",
                "pn-PaperDeliveryCounters", printCapacity] });

        assert.strictEqual(result.statusCode, 200);
        const outerBodyParse = JSON.parse(result.body);
        const body = JSON.parse(outerBodyParse.body);
        assert.strictEqual(body.executionArn, fakeArn);
        
        const calls = sfnMock.commandCalls(StartExecutionCommand);
        assert.strictEqual(calls.length, 1);
        
        const input = JSON.parse(calls[0].args[0].input.input);
        assert.strictEqual(input.PN_DELAYER_PRINTCAPACITY, `1970-01-01;${printCapacity}`);
        assert.strictEqual(input.PN_DELAYER_DELIVERYDATEDAYOFWEEK, "1"); // default
    });

    it("error the step function with no required SFN_ARN provided", async () => {
        const fakeStartDate = new Date();
        sfnMock.on(StartExecutionCommand).resolves({
            startDate: fakeStartDate
        });

        const result = await handler({ operationType: "RUN_ALGORITHM", parameters: ["pn-DelayerPaperDelivery",
                "pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities",
                "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit",
                "pn-PaperDeliveryCounters", "180000"] });

        const body = JSON.parse(result.body);
        assert.strictEqual(body.statusCode, 500);
    });

        it("error the step function with no required parameters provided", async () => {

            const result = await handler({ operationType: "RUN_ALGORITHM", parameters: ["pn-DelayerPaperDelivery"] });
            const body = JSON.parse(result.body);
            assert.strictEqual(body.statusCode, 400);
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

   it("DELETE_DATA returns 500 when unprocessed items remain after retries", async () => {
       const csvPath = path.join(__dirname, "sample.csv");
       const csvData = fs.readFileSync(csvPath, "utf8");
       s3Mock.on(GetObjectCommand).resolves({
           Body: Readable.from([csvData])
       });

       ddbMock.on(QueryCommand).resolves({
           Items: [{
               pk: "2025-08-25~EVALUATE_PRINT_CAPACITY",
               sk: "sk1",
               province: "RM",
               productType: "RS",
               senderPaId: "PaId",
               unifiedDeliveryDriver: "driver1",
               cap: "00178"
           }]
       });

       ddbMock.on(BatchWriteCommand).resolves({
           UnprocessedItems: {
               "pn-DelayerPaperDelivery": [
                   {
                       DeleteRequest: {
                           Key: {
                               pk: "2025-08-25~EVALUATE_PRINT_CAPACITY",
                               sk: "sk1"
                           }
                       }
                   }
               ]
           }
       });

       const result = await handler({
           operationType: "DELETE_DATA",
           parameters: [
               "pn-DelayerPaperDelivery",
               "pn-PaperDeliveryDriverUsedCapacities",
               "pn-PaperDeliveryUsedSenderLimit",
               "pn-PaperDeliveryCounters"
           ]
       });

       assert.strictEqual(result.statusCode, 500);
       const body = JSON.parse(result.body);
       assert.strictEqual(body.message.includes("Delete completed with unprocessed items"), true);
       assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length >= 3, true);
   });

   it("GET_SENDER_LIMIT returns the items and lastEvaluatedKey", async () => {
       const fakeItems = [
           {
               pk: "PA1~PT1~RM",
               deliveryDate: "2025-06-30",
               weeklyEstimate: 100,
               monthlyEstimate: 400,
               originalEstimate: 500,
               paId: "PA1",
               productType: "PT1",
               province: "RM"
           },
           {
               pk: "PA2~PT2~RM",
               deliveryDate: "2025-06-30",
               weeklyEstimate: 150,
               monthlyEstimate: 600,
               originalEstimate: 700,
               paId: "PA2",
               productType: "PT2",
               province: "RM"
           }
       ];

       const fakeLastEvaluatedKey = { pk: "PA2~PT2~RM", deliveryDate: "2025-06-30" };
       ddbMock.on(QueryCommand).resolves({ Items: fakeItems, LastEvaluatedKey: fakeLastEvaluatedKey });

       const params = {"table":"pn-PaperDeliverySenderLimit", "deliveryDate":"2025-06-30", "province":"RM"};
       const result = await handler({ operationType: "GET_SENDER_LIMIT", parameters: params });

       assert.strictEqual(result.statusCode, 200);
       assert.strictEqual(ddbMock.commandCalls(QueryCommand)[0].args[0].input.TableName, "pn-PaperDeliverySenderLimit");
       const body = JSON.parse(result.body);
       assert.strictEqual(body.items.length, 2);
       assert.strictEqual(body.items[0].weeklyEstimate, 100);
       assert.strictEqual(body.items[1].weeklyEstimate, 150);
       assert.deepStrictEqual(body.lastEvaluatedKey, fakeLastEvaluatedKey);
   });

    it("GET_SENDER_LIMIT returns the item with pk", async () => {
         const fakeItem = {
             pk: "PA2~PT2~RM",
             deliveryDate: "2025-06-30",
             weeklyEstimate: 150,
             monthlyEstimate: 600,
             originalEstimate: 700,
             paId: "PA2",
             productType: "PT2",
             province: "RM"
         };

         ddbMock.on(GetCommand).resolves({ Item: fakeItem });

         const params = {
              table: "pn-PaperDeliverySenderLimit",
             deliveryDate: "2025-06-30",
             pk: "PA2~PT2~RM"
         };

         const result = await handler({
             operationType: "GET_SENDER_LIMIT",
             parameters: params
         });

         assert.strictEqual(result.statusCode, 200);
          assert.strictEqual(ddbMock.commandCalls(GetCommand)[0].args[0].input.TableName, "pn-PaperDeliverySenderLimit");
         const body = JSON.parse(result.body);
         assert.strictEqual(body.items.length, 1);
         assert.strictEqual(body.items[0].weeklyEstimate, 150);
     });

   it("GET_SENDER_LIMIT if no items found", async () => {

       ddbMock.on(QueryCommand).resolves({ Items: [] });
       const params = {"table":"pn-PaperDeliverySenderLimit", "deliveryDate":"2025-06-30", "province":"RM"};

       const result = await handler({ operationType: "GET_SENDER_LIMIT", parameters: params });
       const body = JSON.parse(result.body);
       assert.deepStrictEqual(body, { items: [] });
   });

   it("GET_SENDER_LIMIT throws error if parameters are missing", async () => {
       const result = await handler({ operationType: "GET_SENDER_LIMIT", parameters: {} });
       assert.strictEqual(JSON.parse(result.body).message, "Parameters must include table, deliveryDate and (province or pk)");
   });

  it("GET_USED_SENDER_LIMIT returns the items and lastEvaluatedKey", async () => {
      const fakeItems = [
          {
              pk: "PA1~PT1~RM",
              deliveryDate: "2025-06-30",
              numberOfShipment: 120,
              senderLimit: 300,
              paId: "PA1",
              productType: "PT1",
              province: "RM"
          },
          {
              pk: "PA2~PT2~RM",
              deliveryDate: "2025-06-30",
              numberOfShipment: 80,
              senderLimit: 200,
              paId: "PA2",
              productType: "PT2",
              province: "RM"
          }
      ];

      const fakeLastEvaluatedKey = { pk: "PA2~PT2~RM", deliveryDate: "2025-06-30" };
      ddbMock.on(QueryCommand).resolves({ Items: fakeItems, LastEvaluatedKey: fakeLastEvaluatedKey });

      const params = {"deliveryDate":"2025-06-30", "province":"RM", "table":"pn-PaperDeliveryUsedSenderLimit"};
      const result = await handler({ operationType: "GET_USED_SENDER_LIMIT", parameters: params });

      assert.strictEqual(result.statusCode, 200);
      const body = JSON.parse(result.body);
      assert.strictEqual(body.items.length, 2);
      assert.strictEqual(body.items[0].numberOfShipment, 120);
      assert.strictEqual(body.items[1].senderLimit, 200);
      assert.deepStrictEqual(body.lastEvaluatedKey, fakeLastEvaluatedKey);
  });

   it("GET_USED_SENDER_LIMIT returns the item with pk", async () => {
        const fakeItem = {
           pk: "PA1~AR~RM",
           deliveryDate: "2025-06-30",
           numberOfShipment: 150,
           senderLimit: 400,
           paId: "PA1",
           productType: "AR",
           province: "RM"
         };

         ddbMock.on(GetCommand).resolves({ Item: fakeItem });

         const params = {
           deliveryDate: "2025-06-30",
           pk: "PA1~AR~RM",
           table: "pn-PaperDeliveryUsedSenderLimit"
         };

         const result = await handler({
           operationType: "GET_USED_SENDER_LIMIT",
           parameters: params
         });

         assert.strictEqual(result.statusCode, 200);
         const body = JSON.parse(result.body);
         assert.strictEqual(body.items.length, 1);
         assert.strictEqual(body.items[0].numberOfShipment, 150);
         assert.strictEqual(body.items[0].senderLimit, 400);
       });


   it("GET_USED_SENDER_LIMIT with Mock table", async () => {
        const fakeItems = [
      {
        pk: "PA1~PT1~RM",
        deliveryDate: "2025-06-30",
        numberOfShipment: 120,
        senderLimit: 300,
        paId: "PA1",
        productType: "PT1",
        province: "RM"
      },
      {
        pk: "PA2~PT2~RM",
        deliveryDate: "2025-06-30",
        numberOfShipment: 80,
        senderLimit: 200,
        paId: "PA2",
        productType: "PT2",
        province: "RM"
      }
    ];

        const fakeLastEvaluatedKey = { pk: "PA2~PT2~RM", deliveryDate: "2025-06-30" };
        ddbMock.on(QueryCommand).resolves({ Items: fakeItems, LastEvaluatedKey: fakeLastEvaluatedKey });

        const params = {
            deliveryDate: "2025-06-30",
            province: "RM",
            table: "pn-PaperDeliveryUsedSenderLimitMock"
        };

        const result = await handler({
            operationType: "GET_USED_SENDER_LIMIT",
            parameters: params
        });

        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.items.length, 2);
        assert.strictEqual(body.items[0].numberOfShipment, 120);
        assert.strictEqual(body.items[1].senderLimit, 200);
        assert.deepStrictEqual(body.lastEvaluatedKey, fakeLastEvaluatedKey);
  });

  it("GET_USED_SENDER_LIMIT if no items found", async () => {

      ddbMock.on(QueryCommand).resolves({ Items: [] });
      const params = {"deliveryDate":"2025-06-30", "province":"RM", "table": "pn-PaperDeliveryUsedSenderLimit"};

      const result = await handler({ operationType: "GET_USED_SENDER_LIMIT", parameters: params});
      const body = JSON.parse(result.body);
      assert.deepStrictEqual(body, { items: [] });
  });

  it("GET_USED_SENDER_LIMIT throws error if parameters are missing", async () => {
      const result = await handler({ operationType: "GET_USED_SENDER_LIMIT", parameters: {} });
      assert.strictEqual(JSON.parse(result.body).message, "Parameters must include deliveryDate, (province or pk) and table");
  });

   it("should throw error when parameter is missing", async () => {

        const result = await handler({ operationType: "GET_PAPER_DELIVERY", parameters: [] });
        assert.strictEqual(JSON.parse(result.body).message, "Required parameters are [paperDeliveryTableName, deliveryDate, workflowStep]");
   });

   it("should return items when query finds data", async () => {
       const mockItems = [
           { pk: "2025-08-25~EVALUATE_PRINT_CAPACITY", sk: "2025-09-29", province: "RM", productType: "RS" },
           { pk: "2025-08-25~EVALUATE_PRINT_CAPACITY", sk: "2025-09-29", province: "MI", productType: "AR" }
       ];

       ddbMock.on(QueryCommand).resolves({ Items: mockItems });

       const result = await handler({ operationType: "GET_PAPER_DELIVERY", parameters: ["pn-DelayerPaperDelivery","2025-10-03","EVALUATE_PRINT_CAPACITY"]});
       const body = JSON.parse(result.body);

       assert.strictEqual(Array.isArray(body.items), true);
       assert.strictEqual(body.items.length, 2);
       assert.deepStrictEqual(body.items, mockItems);
       assert.strictEqual(body.lastEvaluatedKey, undefined);
   });

   it("should return message when no items found", async () => {
       ddbMock.on(QueryCommand).resolves({ Items: [] });

       const result = await handler({ operationType: "GET_PAPER_DELIVERY", parameters: ["pn-DelayerPaperDelivery","2025-10-03","EVALUATE_PRINT_CAPACITY"]});
       const body = JSON.parse(result.body);

       assert.deepStrictEqual(body, { items: [] });
   });

   it("should use custom limit from environment variable", async () => {
       process.env.PAPER_DELIVERY_QUERYLIMIT = "500";
       ddbMock.on(QueryCommand).resolves({ Items: [] });

       await handler({ operationType: "GET_PAPER_DELIVERY", parameters: ["pn-DelayerPaperDelivery","2025-10-03","EVALUATE_PRINT_CAPACITY"]});

       const calls = ddbMock.commandCalls(QueryCommand);
       const queryParams = calls[0].args[0].input;
       assert.strictEqual(queryParams.Limit, 500);
   });

   it("should return LastEvaluatedKey when pagination is needed", async () => {
       const mockItems = [
           { pk: "2025-09-29~EVALUATE_PRINT_CAPACITY", sk: "2025-09-29" }
       ];
       const mockLastKey = { pk: "2025-09-29~EVALUATE_PRINT_CAPACITY", sk: "2025-09-29" };

       ddbMock.on(QueryCommand).resolves({
           Items: mockItems,
           LastEvaluatedKey: mockLastKey
       });

       const result = await handler({ operationType: "GET_PAPER_DELIVERY", parameters: ["pn-DelayerPaperDelivery","2025-10-03","EVALUATE_PRINT_CAPACITY"]});
       const body = JSON.parse(result.body);

       assert.deepStrictEqual(body.items, mockItems);
       assert.deepStrictEqual(body.lastEvaluatedKey, mockLastKey);
   });

   it("GET_PRESIGNED_URL without checksum", async () => {
     const params = { fileName: "example.csv" };
     const result = await handler({ operationType: "GET_PRESIGNED_URL", parameters: params });
     assert.strictEqual(result.statusCode, 500);
     assert.strictEqual(JSON.parse(result.body).message, "checksumSha256B64 is required");
   });

   it("GET_PRESIGNED_URL with different file type", async () => {
     const params = { fileName: "example.json", checksumSha256B64: "sha256checksumB64" };
     const result = await handler({ operationType: "GET_PRESIGNED_URL", parameters: params });
     assert.strictEqual(result.statusCode, 500);
     assert.strictEqual(JSON.parse(result.body).message, "fileName must end with .csv or .zip");
   });

   it("GET_PRESIGNED_URL download success", async () => {
     const params = { fileName: "example.csv", presignedUrlType: "DOWNLOAD" };
     const result = await handler({ operationType: "GET_PRESIGNED_URL", parameters: params });
     assert.strictEqual(result.statusCode, 200);
     const body = JSON.parse(result.body);
     assert.ok(body.downloadUrl);
     assert.strictEqual(body.key, "example.csv");
     assert.strictEqual(body.expiresIn, 300);
   });

   it("GET_PRESIGNED_URL download without fileName", async () => {
     const params = { presignedUrlType: "DOWNLOAD" };
     const result = await handler({ operationType: "GET_PRESIGNED_URL", parameters: params });
     assert.strictEqual(result.statusCode, 500);
     assert.strictEqual(JSON.parse(result.body).message, "fileName is required");
   });

   it("GET_PRESIGNED_URL presignedUrlType null defaults to UPLOAD", async () => {
     const params = { fileName: "example.csv", checksumSha256B64: "sha256checksumB64" };
     const result = await handler({ operationType: "GET_PRESIGNED_URL", parameters: params });
     assert.strictEqual(result.statusCode, 200);
     const body = JSON.parse(result.body);
     assert.ok(body.uploadUrl);
     assert.ok(body.key);
     assert.ok(body.key.endsWith("-example.csv"));
     assert.strictEqual(body.expiresIn, 300);
   });

  it("GET_PRESIGNED_URL upload success", async () => {
    const params = { fileName: "example.csv", checksumSha256B64: "sha256checksumB64" , presignedUrlType: "UPLOAD"};
    const result = await handler({ operationType: "GET_PRESIGNED_URL", parameters: params });
    assert.strictEqual(result.statusCode, 200);
    const body = JSON.parse(result.body);
    assert.ok(body.uploadUrl);
    assert.ok(body.key);
    assert.ok(body.key.endsWith("-example.csv"));
    assert.strictEqual(body.expiresIn, 300);
    assert.strictEqual(body.requiredHeaders["Content-Type"], "text/csv");
  });

    it("GET_PRESIGNED_URL upload success zip", async () => {
      const params = { fileName: "example.zip", checksumSha256B64: "sha256checksumB64" , presignedUrlType: "UPLOAD"};
      const result = await handler({ operationType: "GET_PRESIGNED_URL", parameters: params });
      assert.strictEqual(result.statusCode, 200);
      const body = JSON.parse(result.body);
      assert.ok(body.uploadUrl);
      assert.ok(body.key);
      assert.strictEqual(body.key, "example.zip");
      assert.strictEqual(body.expiresIn, 300);
      assert.strictEqual(body.requiredHeaders["Content-Type"], "application/zip");
      });

   it("GET_STATUS_EXECUTION returns the status of a successful execution", async () => {
       const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec123";
       const fakeStartDate = new Date();
       const fakeEndDate = new Date();
       sfnMock.on(DescribeExecutionCommand).resolves({
           status: "SUCCEEDED",
           startDate: fakeStartDate,
           stopDate: fakeEndDate
       });

       const result = await handler({ operationType: "GET_STATUS_EXECUTION", parameters: [fakeArn] });
       const body = JSON.parse(result.body);

       assert.strictEqual(body.executionArn, fakeArn);
       assert.strictEqual(body.status, "SUCCEEDED");
       assert.strictEqual(new Date(body.startDate).toISOString(), fakeStartDate.toISOString());
       assert.strictEqual(new Date(body.stopDate).toISOString(), fakeEndDate.toISOString());
       assert.strictEqual(body.error, undefined);
   });

   it("GET_STATUS_EXECUTION Returns an error if execution fails", async () => {
       const fakeArn = "arn:aws:states:...:execution:BatchWorkflowStateMachine:exec456";
       const fakeStartDate = new Date();
       const fakeEndDate = new Date();
       sfnMock.on(DescribeExecutionCommand).resolves({
           status: "FAILED",
           startDate: fakeStartDate,
           stopDate: fakeEndDate
       });

       const result = await handler({ operationType: "GET_STATUS_EXECUTION", parameters: [fakeArn] });
       const body = JSON.parse(result.body);

       assert.strictEqual(body.status, "FAILED");
   });

   it("GET_STATUS_EXECUTION throws error if parameters are missing", async () => {

      const result = await handler({ operationType: "GET_STATUS_EXECUTION", parameters: [] });
      assert.strictEqual(JSON.parse(result.body).message, "Parameter must be [executionArn]");
   });

    it("GET_DECLARED_CAPACITY returns the item close range", async () => {
        const fakeItem = {
            pk: "tenderId1~unifiedDeliveryDriver1~geoKey1",
            tenderIdGeoKey: "tenderId1~geoKey1",
            activationDateFrom: "2025-01-01T00:00:00Z",
            activationDateTo: "2025-12-31T00:00:00Z",
            tenderId: "tenderId1",
            unifiedDeliveryDriver: "unifiedDeliveryDriver1",
            geoKey: "geoKey1",
            capacity: 100
        };

        lambdaMock.on(InvokeCommand).resolves({
            Payload: Buffer.from(JSON.stringify({
                body: { tenderId: "tenderId1" }
            }))
        });

        ddbMock.on(QueryCommand).resolves({ Items: [fakeItem] });

        const params = ["pn-PaperDeliveryDriverCapacities", "geoKey1", "2025-06-30T00:00:00Z"];
        const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });
        const body = JSON.parse(result.body);

        assert.strictEqual(body.items.length, 1);
        assert.deepStrictEqual(body.items[0].capacity, 100);
    });

    it("GET_DECLARED_CAPACITY returns the item open range", async () => {
        const fakeItem = {
            pk: "tenderId1~unifiedDeliveryDriver1~geoKey1",
            tenderIdGeoKey: "tenderId1~geoKey1",
            activationDateFrom: "2025-01-01T00:00:00Z",
            tenderId: "tenderId1",
            unifiedDeliveryDriver: "unifiedDeliveryDriver1",
            geoKey: "geoKey1",
            capacity: 50
        };

        lambdaMock.on(InvokeCommand).resolves({
            Payload: Buffer.from(JSON.stringify({
                body: { tenderId: "tenderId1" }
            }))
        });

        ddbMock.on(QueryCommand).resolves({ Items: [fakeItem] });

        const params = ["pn-PaperDeliveryDriverCapacities", "geoKey1", "2025-06-30T00:00:00Z"];
        const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });
        const body = JSON.parse(result.body);

        assert.strictEqual(body.items.length, 1);
        assert.deepStrictEqual(body.items[0].capacity, 50);
    });

    it("GET_DECLARED_CAPACITY returns empty array if no item found", async () => {
        lambdaMock.on(InvokeCommand).resolves({
            Payload: Buffer.from(JSON.stringify({
                body: { tenderId: "tenderId1" }
            }))
        });

        ddbMock.on(QueryCommand).resolves({ Items: [] });

        const params = ["pn-PaperDeliveryDriverCapacities", "geoKey1", "2025-06-30T00:00:00Z"];
        const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });

        const body = JSON.parse(result.body);
        assert.deepStrictEqual(body.items, []);
    });

    it("GET_DECLARED_CAPACITY throws error if parameters are missing", async () => {
        const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: [] });
        assert.strictEqual(JSON.parse(result.body).message, "Parameters must be [paperDeliveryDriverCapacitiesTabelName, province, deliveryDate]");
    });

    it("GET_DECLARED_CAPACITY throws error if no active tender found", async () => {
        lambdaMock.on(InvokeCommand).resolves({
            Payload: Buffer.from(JSON.stringify({
                body: { tenderId: null }
            }))
        });

        const params = ["pn-PaperDeliveryDriverCapacities", "geoKey1", "2025-06-30T00:00:00Z"];
        const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });
        assert.strictEqual(JSON.parse(result.body).message, "No active tender found");
    });

    it("GET_DECLARED_CAPACITY groups by driver and returns most recent activation date", async () => {
        const fakeItems = [
            {
                pk: "tenderId1~driver1~geoKey1",
                tenderIdGeoKey: "tenderId1~geoKey1",
                activationDateFrom: "2025-01-01T00:00:00Z",
                activationDateTo: "2025-12-31T00:00:00Z",
                tenderId: "tenderId1",
                unifiedDeliveryDriver: "driver1",
                geoKey: "geoKey1",
                capacity: 100
            },
            {
                pk: "tenderId1~driver1~geoKey1",
                tenderIdGeoKey: "tenderId1~geoKey1",
                activationDateFrom: "2025-06-01T00:00:00Z",
                activationDateTo: "2025-12-31T00:00:00Z",
                tenderId: "tenderId1",
                unifiedDeliveryDriver: "driver1",
                geoKey: "geoKey1",
                capacity: 150
            },
            {
                pk: "tenderId1~driver2~geoKey1",
                tenderIdGeoKey: "tenderId1~geoKey1",
                activationDateFrom: "2025-01-01T00:00:00Z",
                tenderId: "tenderId1",
                unifiedDeliveryDriver: "driver2",
                geoKey: "geoKey1",
                capacity: 200
            }
        ];

        lambdaMock.on(InvokeCommand).resolves({
            Payload: Buffer.from(JSON.stringify({
                body: { tenderId: "tenderId1" }
            }))
        });

        ddbMock.on(QueryCommand).resolves({ Items: fakeItems });

        const params = ["pn-PaperDeliveryDriverCapacities", "geoKey1", "2025-06-30T00:00:00Z"];
        const result = await handler({ operationType: "GET_DECLARED_CAPACITY", parameters: params });
        const body = JSON.parse(result.body);

        assert.strictEqual(body.items.length, 2);
        const driver1Item = body.items.find(item => item.unifiedDeliveryDriver === "driver1");
        const driver2Item = body.items.find(item => item.unifiedDeliveryDriver === "driver2");

        assert.deepStrictEqual(driver1Item.capacity, 150);
        assert.deepStrictEqual(driver1Item.activationDateFrom, "2025-06-01T00:00:00Z");
        assert.deepStrictEqual(driver2Item.capacity, 200);
    });

  it("INSERT_MOCK_SENDER_LIMIT invokes file-ready lambda with presigned download url", async () => {
    process.env.EVENTFILEREADY_LAMBDA_ARN =
      "arn:aws:lambda:eu-south-1:123456789012:function:eventFileReady";

    const result = await handler({
      operationType: "INSERT_MOCK_SENDER_LIMITS",
      parameters: ["sender-limit.zip"],
    });

    assert.strictEqual(result.statusCode, 200);

    const body = JSON.parse(result.body);
    assert.strictEqual(body.message, "Import senderLimit process started");

    const calls = lambdaMock.commandCalls(InvokeCommand);
    assert.strictEqual(calls.length, 1);

    const input = calls[0].args[0].input;
    assert.strictEqual(input.FunctionName, process.env.EVENTFILEREADY_LAMBDA_ARN);
    assert.strictEqual(input.InvocationType, "RequestResponse");

    const payload = JSON.parse(input.Payload);
    assert.strictEqual(payload.httpMethod, "POST");
    assert.strictEqual(payload.resource, "/file-ready-event");
    const payloadBody = JSON.parse(payload.body);
    assert.strictEqual(payloadBody.mock, true);
    assert.ok(payloadBody.downloadUrl);
  });

   it("INSERT_MOCK_CAPACITIES should batch-write items to DynamoDB", async () => {
      const csvPath = path.join(__dirname, "capacitySample.csv");
      const csvData = fs.readFileSync(csvPath, "utf8");
      s3Mock.on(GetObjectCommand).resolves({
          Body: Readable.from([csvData])
      });
      ddbMock.on(BatchWriteCommand).resolves({});
      lambdaMock.on(InvokeCommand).resolves({
          Payload: Buffer.from(JSON.stringify({ body: { tenderId: "20250319" } }))
      });

      const result = await handler({ operationType: "INSERT_MOCK_CAPACITIES", parameters: ["pn-PaperDeliveryDriverCapacities","file.csv"] });
      const body = JSON.parse(result.body);
      assert.strictEqual(result.statusCode, 200);
      assert.strictEqual(body.message, "CSV imported successfully");
      assert.strictEqual(body.processed, 4);
      assert.strictEqual(ddbMock.commandCalls(BatchWriteCommand).length > 0, true);
   });

   it("INSERT_MOCK_CAPACITIES salta i record non validi", async () => {
       const csvData = "unifiedDeliveryDriver;geoKey;activationDateFrom;capacity;peakCapacity;products\n ;87100;2024-06-01;10;20;[\"AR\"]";
       s3Mock.on(GetObjectCommand).resolves({
           Body: Readable.from([csvData])
       });
       ddbMock.on(BatchWriteCommand).resolves({});
       lambdaMock.on(InvokeCommand).resolves({
           Payload: Buffer.from(JSON.stringify({ body: { tenderId: "20250319" } }))
       });

       const result = await handler({ operationType: "INSERT_MOCK_CAPACITIES", parameters: ["pn-paperDeliveryDriverCapacityMock", "file.csv"] });
       const body = JSON.parse(result.body);
       assert.strictEqual(result.statusCode,500);
       assert.strictEqual(body.message, "Field unifiedDeliveryDriver is required and cannot be empty");
   });

    it("GET_COUNTERS PRINT record found", async () => {
      const fakeItem = {
         "pk": "PRINT",
         "sk": "2025-11-24",
         "dailyExecutionCounter": 4,
         "dailyExecutionNumber": 4,
         "dailyPrintCapacity": 20,
         "numberOfShipments": 70,
         "sentToNextWeek": 0,
         "sentToPhaseTwo": 11,
         "ttl": 1766571878068,
         "weeklyPrintCapacity": 140
      }

      ddbMock.on(GetCommand).resolves({ Item: fakeItem });

      const result = await handler({
        operationType: "GET_COUNTERS",
        parameters: {
          table: "pn-PaperDeliveryCounters",
          counterType: "PRINT",
          deliveryDate: "2025-11-24"
        }
      });

      assert.strictEqual(result.statusCode, 200);
      const body = JSON.parse(result.body);
      assert.strictEqual(body.items[0].dailyExecutionNumber, 4);
    });

    it("GET_COUNTERS PRINT no record", async () => {
      ddbMock.on(GetCommand).resolves({});

      const result = await handler({
        operationType: "GET_COUNTERS",
        parameters: {
          table: "pn-PaperDeliveryCounters",
          counterType: "PRINT",
          deliveryDate: "2025-11-24"
        }
      });

      assert.strictEqual(result.statusCode, 200);
      const body = JSON.parse(result.body);
      assert.strictEqual(body.items.length, 0);
    });

    it("GET_COUNTERS SUM_ESTIMATES base", async () => {
      ddbMock.on(QueryCommand).resolves({
        Items: [{ sk: "SUM_ESTIMATES" }],
        LastEvaluatedKey: { pk: "lek" }
      });

      const result = await handler({
        operationType: "GET_COUNTERS",
        parameters: {
          table: "pn-PaperDeliveryCounters",
          counterType: "SUM_ESTIMATES",
          deliveryDate: "2025-11-24"
        }
      });

      assert.strictEqual(result.statusCode, 200);
      const body = JSON.parse(result.body);
      assert.strictEqual(body.items.length, 1);
      assert.deepStrictEqual(body.lastEvaluatedKey, { pk: "lek" });
    });


    it("GET_COUNTERS SUM_ESTIMATES error province without product", async () => {
      const result = await handler({
        operationType: "GET_COUNTERS",
        parameters: {
          table: "pn-PaperDeliveryCounters",
          counterType: "SUM_ESTIMATES",
          deliveryDate: "2025-11-24",
          province: "MI"
        }
      });

      assert.strictEqual(result.statusCode, 500);
      const body = JSON.parse(result.body);
      assert.match(body.message, /productType is required/);
    });

    it("GET_COUNTERS EXCLUDE base", async () => {
      ddbMock.on(QueryCommand).resolves({
        Items: [{ sk: "EXCLUDE" }]
      });

      const result = await handler({
        operationType: "GET_COUNTERS",
        parameters: {
          table: "pn-PaperDeliveryCounters",
          counterType: "EXCLUDE",
          deliveryDate: "2025-11-24"
        }
      });

      assert.strictEqual(result.statusCode, 200);
      const body = JSON.parse(result.body);
      assert.strictEqual(body.items.length, 1);
    });

    it("GET_COUNTERS EXCLUDE getCommand", async () => {
      const fakeItem = { sk: "EXCLUDE~MI~RS" };

      ddbMock.on(GetCommand).resolves({ Item: fakeItem });

      const result = await handler({
        operationType: "GET_COUNTERS",
        parameters: {
          table: "pn-PaperDeliveryCounters",
          counterType: "EXCLUDE",
          deliveryDate: "2025-11-24",
          province: "MI",
          productType: "RS"
        }
      });

      assert.strictEqual(result.statusCode, 200);
      const body = JSON.parse(result.body);
      assert.strictEqual(body.items[0].sk, "EXCLUDE~MI~RS");
    });

    it("GET_COUNTERS EXCLUDE errore product without province", async () => {
      const result = await handler({
        operationType: "GET_COUNTERS",
        parameters: {
          table: "pn-PaperDeliveryCounters",
          counterType: "EXCLUDE",
          deliveryDate: "2025-11-24",
          productType: "RS"
        }
      });

      assert.strictEqual(result.statusCode, 500);
      const body = JSON.parse(result.body);
      assert.match(body.message, /province is required/);
    });

    it("GET_COUNTERS SENDER_PRIORITY record found", async () => {
        const fakeItem = {
            pk: "2025-11-24",
            sk: "SENDER_PRIORITY~paId1",
            priorities: new Set([30, 80])
        };

        ddbMock.on(GetCommand).resolves({ Item: fakeItem });

        const result = await handler({
            operationType: "GET_COUNTERS",
            parameters: {
                table: "pn-PaperDeliveryCounters",
                counterType: "SENDER_PRIORITY",
                paId: "paId1",
                deliveryDate: "2025-11-24"
            }
        });

        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.items.length, 1);
        assert.deepStrictEqual(body.items[0].priorities, [30, 80]);
        assert.strictEqual(body.items[0].sk, "SENDER_PRIORITY~paId1");
    });

    it("GET_COUNTERS SENDER_PRIORITY no record", async () => {
        ddbMock.on(GetCommand).resolves({});

        const result = await handler({
            operationType: "GET_COUNTERS",
            parameters: {
                table: "pn-PaperDeliveryCounters",
                counterType: "SENDER_PRIORITY",
                paId: "paId1",
                deliveryDate: "2025-11-24"
            }
        });

        assert.strictEqual(result.statusCode, 200);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.items.length, 0);
    });

    it("GET_COUNTERS SENDER_PRIORITY paId undefined", async () => {
        const result = await handler({
            operationType: "GET_COUNTERS",
            parameters: {
                table: "pn-PaperDeliveryCounters",
                counterType: "SENDER_PRIORITY",
                deliveryDate: "2025-11-24"
            }
        });

        assert.strictEqual(result.statusCode, 500);
        const body = JSON.parse(result.body);
        assert.strictEqual(body.message, "Required parameter [paId]");
    });

    it("GET_RESIDUAL_PAPERS returns downloadUrl on success", async () => {
      athenaMock.on(StartQueryExecutionCommand).resolves({
        QueryExecutionId: "exec-123",
      });

      athenaMock.on(GetQueryExecutionCommand).resolves({
        QueryExecution: {
          Status: { State: "SUCCEEDED" },
          ResultConfiguration: {
            OutputLocation: "s3://test-bucket/residual-papers/exec-123.csv",
          },
        },
      });

      const csvContent =
        "iun,created_at,updated_at,status,sender,address,cap,attempt,product\n" +
        "PREPARE_ANALOG_DOMICILE.IUN_YDTA-XNPA-UXVL-202506-M-1.RECINDEX_0.ATTEMPT_0,1970-01-05T00:00:00Z,1970-01-05T00:00:00Z,AR,idMittente1,NA,80124,0,YDTA-XNPA-UXVL-202506-M-1\n";

      s3Mock.on(GetObjectCommand).resolves({
        Body: Readable.from([csvContent]),
      });

      s3Mock.on(PutObjectCommand).resolves({});
      s3Mock.on(DeleteObjectCommand).resolves({});

      const result = await handler({
        operationType: "GET_RESIDUAL_PAPERS",
        parameters: ["pn_delayer_paper_delivery_json_view", "2025-06-16"],
      });

      assert.strictEqual(result.statusCode, 200);

      const body = JSON.parse(result.body);
      assert.ok(body.downloadUrl);
      assert.strictEqual(body.expiresIn, 300);
    });
});