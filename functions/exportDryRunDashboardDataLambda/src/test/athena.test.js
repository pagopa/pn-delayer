const { expect } = require("chai");
const proxyquire = require("proxyquire").noCallThru();
const sinon = require("sinon");

class MockStartQueryExecutionCommand {
  constructor(input) {
    this.input = input;
  }
}

class MockGetQueryExecutionCommand {
  constructor(input) {
    this.input = input;
  }
}

class MockGetQueryResultsCommand {
  constructor(input) {
    this.input = input;
  }
}

function buildAthenaMock(sequence) {
  let index = 0;
  return {
    async send() {
      return sequence[index++];
    }
  };
}

describe("queryExecution", () => {
  let clock;

  beforeEach(() => {
    clock = sinon.useFakeTimers(); // evita ritardi reali
  });

  afterEach(() => {
    clock.restore();
  });

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

    const { queryExecution } = proxyquire("../app/lib/athena", {
      "@aws-sdk/client-athena": {
        AthenaClient: function () {
          return athenaMock;
        },
        StartQueryExecutionCommand: MockStartQueryExecutionCommand,
        GetQueryExecutionCommand: MockGetQueryExecutionCommand,
        GetQueryResultsCommand: MockGetQueryResultsCommand
      }
    });

    const promise = queryExecution("SELECT 1", "mydb", outputLocation);

    await clock.tickAsync(5000); // FORZA l’avanzamento del tempo finto
    await clock.tickAsync(5000);

    await promise; // ora non c’è timeout
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

    const { queryExecution } = proxyquire("../app/lib/athena", {
      "@aws-sdk/client-athena": {
        AthenaClient: function () {
          return athenaMock;
        },
        StartQueryExecutionCommand: MockStartQueryExecutionCommand,
        GetQueryExecutionCommand: MockGetQueryExecutionCommand
      }
    });

    let error;
    const promise = queryExecution("SELECT 1", "mydb", "s3://bucket/output/");

    await clock.tickAsync(5000);

    try {
      await promise;
    } catch (err) {
      error = err;
    }

    expect(error.message).to.include("FAILED");
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

    const { queryExecution } = proxyquire("../app/lib/athena", {
      "@aws-sdk/client-athena": {
        AthenaClient: function () {
          return athenaMock;
        },
        StartQueryExecutionCommand: MockStartQueryExecutionCommand,
        GetQueryExecutionCommand: MockGetQueryExecutionCommand
      }
    });

    let error;
    const promise = queryExecution("workgroup-test", "SELECT 1", "mydb", "s3://bucket/output/");

    await clock.tickAsync(5000);

    try {
      await promise;
    } catch (err) {
      error = err;
    }

    expect(error.message).to.include("CANCELLED");
  });
});
