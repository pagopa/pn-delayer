const sinon = require("sinon");
const { expect } = require("chai");
const proxyquire = require("proxyquire");

describe("eventHandler", () => {
  let eventHandler;
  let prepareQueryConditionStub;
  let queryExecutionStub;
  let copyS3ObjectStub;
  let deleteS3ObjectStub;
  let getCurrentMondayStub;

  beforeEach(() => {
    // Stub delle dipendenze
    prepareQueryConditionStub = sinon.stub().returns("SELECT test_query");
    queryExecutionStub = sinon.stub().resolves("s3://bucket/athena_results/tmp.csv");
    copyS3ObjectStub = sinon.stub().resolves({});
    deleteS3ObjectStub = sinon.stub().resolves({});
    getCurrentMondayStub = sinon.stub().returns("2024-01-01");

    // Proxyquire il modulo eventHandler sostituendo le dipendenze
    eventHandler = proxyquire("../app/eventHandler.js", {
      "../app/lib/utils": {
        prepareQueryCondition: prepareQueryConditionStub,
        getCurrentMonday: getCurrentMondayStub
      },
      "../app/lib/athena": {
        queryExecution: queryExecutionStub
      },
      "../app/lib/s3": {
        copyS3Object: copyS3ObjectStub,
        deleteS3Object: deleteS3ObjectStub
      }
    });
  });

  afterEach(() => sinon.restore());

  /* ----------------- INTEGRATION TEST ----------------- */
  it("esegue handleEvent con flusso completo", async () => {
    process.env.ATHENA_DATABASE_NAME = "testdb";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.SPECIFIC_DATE = "2024-01-01";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";

    await eventHandler.handleEvent({});

    // Verifica che le dipendenze siano state chiamate
    expect(prepareQueryConditionStub.callCount).to.equal(1); // due query
    expect(queryExecutionStub.called).to.be.true;
    expect(copyS3ObjectStub.called).to.be.true;
    expect(deleteS3ObjectStub.callCount).to.be.greaterThan(1);
  });

  it("usa getCurrentMonday se SPECIFIC_DATE non è impostata", async () => {
    process.env.ATHENA_DATABASE_NAME = "testdb";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";
    delete process.env.SPECIFIC_DATE;

    await eventHandler.handleEvent({});
    expect(getCurrentMondayStub.calledOnce).to.be.true;
  });

  it("gestisce queryExecution null (nessun risultato)", async () => {
    queryExecutionStub.resolves(null);

    process.env.ATHENA_DATABASE_NAME = "db";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.SPECIFIC_DATE = "2024-01-01";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";

    await eventHandler.handleEvent({});

    // Se queryExecution ritorna null, copyS3Object non dovrebbe essere chiamato
    expect(copyS3ObjectStub.notCalled).to.be.true;
  });
});
