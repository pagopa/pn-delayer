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
            { requestId: "1" , iun: "iun1"},
            { requestId: "2", iun: "iun2"},
        ];
        sqsMock.onCall(0).resolves({
            Successful: [{ Id: "0" }, { Id: "1" }],
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
            requestId: `req${i + 1}` ,
            iun: `iun${i + 1}`,
        }));

        sqsMock.onCall(0).resolves({
            Successful: [{ Id: "0" }, { Id: "1" },{ Id: "2" },{ Id: "3" },{ Id: "4" },{ Id: "8" },{ Id: "9" }],
            Failed: [{ Id: "5" },{ Id: "6" },{ Id: "7" }],
        }).onCall(1).resolves({
            Successful: [{ Id: "0" },{ Id: "3" },{ Id: "4" }],
            Failed: [{ Id: "1" },{ Id: "2" }],
        });

        const results = await prepareAndSendSqsMessages(mockItems);

        sinon.assert.callCount(sqsMock, 2);
        sinon.assert.calledWith(sqsMock, sinon.match.instanceOf(SendMessageBatchCommand));
        expect(results.successes).to.deep.equal(["req1","req2","req3","req4","req5","req9","req10","req11","req14","req15"]);
        expect(results.failures).to.deep.equal(["req6","req7","req8","req12","req13"]);
    });

    it("should handle failed messages", async () => {
        const mockItems = [
            { requestId: "1" , iun: "iun1"},
            { requestId: "2", iun: "iun2"},
        ];
        sqsMock.onCall(0).resolves({
            Successful: [{ Id: "0" }],
            Failed: [ { Id: "1" }],
        });

        const results = await prepareAndSendSqsMessages(mockItems);

        sinon.assert.callCount(sqsMock, 1);
        sinon.assert.calledWith(sqsMock, sinon.match.instanceOf(SendMessageBatchCommand));
        expect(results.successes).to.deep.equal(["1" ]);
        expect(results.failures).to.deep.equal(["2"]);
    });

    it("should handle errors during SendMessageBatch", async () => {
        const mockItems = [
            { requestId: "1", iun: "iun1"},
            { requestId: "2", iun: "iun2"},
        ];
        sqsMock.onCall(0).rejects(new Error("Simulated sqs Error"));

        const results = await prepareAndSendSqsMessages(mockItems);

        sinon.assert.callCount(sqsMock, 1);
        sinon.assert.calledWith(sqsMock, sinon.match.instanceOf(SendMessageBatchCommand));
        expect(results.successes).to.deep.equal([]);
        expect(results.failures).to.deep.equal(["1","2" ]);
    });
});
