const { Readable } = require("stream");
const { handler } = require("../../index");
const fs = require("fs");
const path = require("path");

process.env.BUCKET_NAME = "test-bucket";
process.env.OBJECT_KEY = "test-key.csv";

const { mockClient } = require("aws-sdk-client-mock");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const {
    DynamoDBDocumentClient,
    BatchWriteCommand
} = require("@aws-sdk/lib-dynamodb");

const s3Mock = mockClient(S3Client);
const ddbMock = mockClient(DynamoDBDocumentClient);

describe("Lambda CSV import", () => {
    beforeEach(() => {
        s3Mock.reset();
        ddbMock.reset();
    });

    it("should batch-write items to DynamoDB", async () => {
        const csvPath = path.join(__dirname, "sample.csv");
        const csvData = fs.readFileSync(csvPath, "utf8");
        s3Mock.on(GetObjectCommand).resolves({
            Body: Readable.from([csvData])
        });
        ddbMock.on(BatchWriteCommand).resolves({});

        const result = await handler({ operationType: "IMPORT_DATA", parameters: [] });

        expect(result.statusCode).toBe(200);
        expect(ddbMock.commandCalls(BatchWriteCommand).length).toBeGreaterThan(0);
    });
});