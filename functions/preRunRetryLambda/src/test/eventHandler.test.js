const { expect } = require("chai");
const sinon = require("sinon");
const LambdaTester = require("lambda-tester");
const proxyquire = require("proxyquire");

const { LocalDate } = require("@js-joda/core");

describe("eventHandler", () => {
  let executionStub;
  let schedulerStub;
  let handler;

  beforeEach(() => {
    executionStub = sinon.stub();
    schedulerStub = sinon.stub();

    handler = proxyquire("../app/eventHandler", {
      "../app/lib/stepFunction": {
        executionWithCurrentDateExists: executionStub,
      },
      "../app/lib/eventBridge": {
        getActiveScheduler: schedulerStub,
      },
      "../app/lib/utils": {
        normalizeToLocalDate: () => LocalDate.parse("2025-01-20"),
      },
    }).handleEvent;

  });

  it("should return executeRetryAlgorithm false when execution already exists", async () => {
    executionStub.resolves(false);

    await LambdaTester(handler)
      .event({})
      .expectResult(result => {
        expect(result.executeRetryAlgorithm).to.equal(false);
      });
  });

  it("should return scheduler info when execution can run", async () => {
    executionStub.resolves(true);
    schedulerStub.resolves({
      name: "test-scheduler",
      scheduleExpression: "cron(0 10 ? * MON *)",
    });

    await LambdaTester(handler)
      .event({})
      .expectResult(result => {
        expect(result.executeRetryAlgorithm).to.equal(true);
        expect(result.schedulerName).to.equal("test-scheduler");
        expect(result.schedulerExpression).to.equal("cron(0 10 ? * MON *)");
        expect(result.deliveryDate.toString()).to.equal("2025-01-20");
      });
  });

  it("should skip execution check when skipStepExecutionCheck is true", async () => {
    schedulerStub.resolves({
      name: "scheduler",
      expression: "cron(0 10 ? * MON *)",
    });

    await LambdaTester(handler)
      .event({ skipStepExecutionCheck: true })
      .expectResult(result => {
        expect(executionStub.called).to.equal(false);
        expect(result.executeRetryAlgorithm).to.equal(true);
      });
  });
});
