const assert = require("assert");
const proxyquire = require("proxyquire").noCallThru();

describe("queryExecution", () => {
  let now, tickedMs;
  let clock;

  // Fake timer implementation
  beforeEach(() => {
    now = Date.now();
    tickedMs = 0;
    global._originalSetTimeout = global.setTimeout;
    global.setTimeout = (fn, ms) => {
      tickedMs += ms;
      return fn();
    };
  });

  afterEach(() => {
    global.setTimeout = global._originalSetTimeout;
    delete global._originalSetTimeout;
  });

  function buildAthenaMock(sequence) {
    let index = 0;
    return {
      async send() {
        return sequence[index++];
      }
    };
  }

  it("esegue correttamente una query che va in SUCCEEDED", async () => {
    const outputLocation = "s3://bucket/results/success.csv";
    const mockSeq = [
      { QueryExecutionId: "123" },
      {
        QueryExecution: {
          Status: { State: "RUNNING" },
          ResultConfiguration: { OutputLocation: outputLocation }
        }
      },
      {
        QueryExecution: {
          Status: { State: "SUCCEEDED" },
          ResultConfiguration: { OutputLocation: outputLocation }
        }
      }
    ];
    const athenaMock = buildAthenaMock(mockSeq);
    const { queryExecution } = proxyquire("../../app/lib/athena", {
      "@aws-sdk/client-athena": {
        AthenaClient: function () { return athenaMock; },
        StartQueryExecutionCommand: function (input) { this.input = input; },
        GetQueryExecutionCommand: function (input) { this.input = input; },
        GetQueryResultsCommand: function (input) { this.input = input; }
      }
    });
    const result = await queryExecution("workgroup-test", "SELECT 1", "mydb", outputLocation);
    assert.strictEqual(result, outputLocation);
  });

  it("lancia errore se la query va in FAILED", async () => {
    const mockSeq = [
      { QueryExecutionId: "123" },
      {
        QueryExecution: {
          Status: { State: "FAILED" },
          ResultConfiguration: { OutputLocation: "s3://dummy" }
        }
      }
    ];
    const athenaMock = buildAthenaMock(mockSeq);
    const { queryExecution } = proxyquire("../../app/lib/athena", {
      "@aws-sdk/client-athena": {
        AthenaClient: function () { return athenaMock; },
        StartQueryExecutionCommand: function (input) { this.input = input; },
        GetQueryExecutionCommand: function (input) { this.input = input; }
      }
    });
    let error;
    try {
      await queryExecution("workgroup-test", "SELECT 1", "mydb", "s3://bucket/output/");
    } catch (err) {
      error = err;
    }
    assert.ok(error && error.message.includes("FAILED"));
  });

  it("lancia errore se la query va in CANCELLED", async () => {
    const mockSeq = [
      { QueryExecutionId: "123" },
      {
        QueryExecution: {
          Status: { State: "CANCELLED" },
          ResultConfiguration: { OutputLocation: "s3://dummy" }
        }
      }
    ];
    const athenaMock = buildAthenaMock(mockSeq);
    const { queryExecution } = proxyquire("../../app/lib/athena", {
      "@aws-sdk/client-athena": {
        AthenaClient: function () { return athenaMock; },
        StartQueryExecutionCommand: function (input) { this.input = input; },
        GetQueryExecutionCommand: function (input) { this.input = input; }
      }
    });
    let error;
    try {
      await queryExecution("workgroup-test", "SELECT 1", "mydb", "s3://bucket/output/");
    } catch (err) {
      error = err;
    }
    assert.ok(error && error.message.includes("CANCELLED"));
  });
});
