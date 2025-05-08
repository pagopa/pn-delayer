const { prepareAndSendSqsMessages} = require("../../app/lib/sqsSender");
const sinon = require('sinon');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const { expect } = chai;
const { SQSClient , SendMessageBatchCommand} = require("@aws-sdk/client-sqs");


describe("prepareAndSendSqsMessages", () => {
    let sqsMock;

    beforeEach(() => {
        sqsMock = sinon.stub();
        sinon.replace(SQSClient.prototype, 'send', sqsMock);
    });

    afterEach(() => {
        sinon.restore();
    });

    it("should send messages successfully", async () => {
        const mockItems = [
            { requestId: { S: "1" }, iun: { S: "iun1" } },
            { requestId: { S: "2" }, iun: { S: "iun2" } },
        ];
        sqsMock.onCall(0).resolves({
            Successful: [{ Id: "1" }, { Id: "2" }],
            Failed: [],
        });

        const results = await prepareAndSendSqsMessages(mockItems);

        sinon.assert.callCount(sqsMock, 1);
        sinon.assert.calledWith(sqsMock, sinon.match.instanceOf(SendMessageBatchCommand));
        expect(results.successes).to.deep.equal(["1","2"]);
        expect(results.failures).to.deep.equal([]);
    });

    it("should send messages multiple chunk", async () => {

        const mockItems = Array.from({ length: 15 }, (_, i) => ({
            requestId: { S: `req${i + 1}` },
            iun: { S: `iun${i + 1}` },
        }));

        sqsMock.onCall(0).resolves({
            Successful: [{ Id: "req1" }, { Id: "req2" },{ Id: "req3" },{ Id: "req4" },{ Id: "req5" },{ Id: "req9" },{ Id: "req10" }],
            Failed: [{ Id: "req6" },{ Id: "req7" },{ Id: "req8" }],
        }).onCall(1).resolves({
            Successful: [{ Id: "req11" },{ Id: "req14" },{ Id: "req15" }],
            Failed: [{ Id: "req12" },{ Id: "req13" }],
        });

        const results = await prepareAndSendSqsMessages(mockItems);

        sinon.assert.callCount(sqsMock, 2);
        sinon.assert.calledWith(sqsMock, sinon.match.instanceOf(SendMessageBatchCommand));
        expect(results.successes).to.deep.equal(["req1","req2","req3","req4","req5","req9","req10","req11","req14","req15"]);
        expect(results.failures).to.deep.equal(["req6","req7","req8","req12","req13"]);
    });

    it("should handle failed messages", async () => {
        const mockItems = [
            { requestId: { S: "1" }, iun: { S: "iun1" } },
            { requestId: { S: "2" }, iun: { S: "iun2" } },
        ];
        sqsMock.onCall(0).resolves({
            Successful: [{ Id: "1" }],
            Failed: [ { Id: "2" }],
        });

        const results = await prepareAndSendSqsMessages(mockItems);

        sinon.assert.callCount(sqsMock, 1);
        sinon.assert.calledWith(sqsMock, sinon.match.instanceOf(SendMessageBatchCommand));
        expect(results.successes).to.deep.equal(["1" ]);
        expect(results.failures).to.deep.equal(["2"]);
    });

    it("should handle errors during SendMessageBatch", async () => {
        const mockItems = [
            { requestId: { S: "1" }, iun: { S: "iun1" } },
            { requestId: { S: "2" }, iun: { S: "iun2" } },
        ];
        sqsMock.onCall(0).rejects(new Error("Simulated sqs Error"));

        const results = await prepareAndSendSqsMessages(mockItems);

        sinon.assert.callCount(sqsMock, 1);
        sinon.assert.calledWith(sqsMock, sinon.match.instanceOf(SendMessageBatchCommand));
        expect(results.successes).to.deep.equal([]);
        expect(results.failures).to.deep.equal(["1","2" ]);
    });
});
