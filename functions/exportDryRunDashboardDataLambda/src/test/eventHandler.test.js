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
  let getCurrentDateStub;
  let getNextMonthDateStub;

  beforeEach(() => {
    // Stub delle dipendenze
    prepareQueryConditionStub = sinon.stub().returns("SELECT test_query");
    queryExecutionStub = sinon.stub().resolves("s3://bucket/athena_results/tmp.csv");
    copyS3ObjectStub = sinon.stub().resolves({});
    deleteS3ObjectStub = sinon.stub().resolves({});
    getCurrentMondayStub = sinon.stub().returns("2024-01-01");
    getCurrentDateStub = sinon.stub().returns("2024-01-15");
    getNextMonthDateStub = sinon.stub().returns("2024-02");

    // Proxyquire il modulo eventHandler sostituendo le dipendenze
    eventHandler = proxyquire("../app/eventHandler.js", {
      "../app/lib/utils": {
        prepareQueryCondition: prepareQueryConditionStub,
        getCurrentMonday: getCurrentMondayStub,
        getCurrentDate: getCurrentDateStub,
        getNextMonthDate: getNextMonthDateStub,
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

  afterEach(() => {
    sinon.restore();
    delete process.env.ATHENA_DATABASE_NAME;
    delete process.env.MONITORING_BUCKET_NAME;
    delete process.env.SPECIFIC_DATE;
    delete process.env.SPECIFIC_DAILY_DATE;
    delete process.env.SPECIFIC_MONTHLY_DATE;
    delete process.env.ATHENA_WORKGROUP_NAME;
  });

  /* ----------------- INTEGRATION TEST ----------------- */
  it("esegue handleEvent con flusso completo", async () => {
    process.env.ATHENA_DATABASE_NAME = "testdb";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.SPECIFIC_DATE = "2024-01-01";
    process.env.SPECIFIC_DAILY_DATE = "2024-01-15";
    process.env.SPECIFIC_MONTHLY_DATE = "2024-01";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";

    await eventHandler.handleEvent({});

    // Verifica che le dipendenze siano state chiamate
    expect(prepareQueryConditionStub.callCount).to.equal(4); // quattro query
    expect(queryExecutionStub.called).to.be.true;
    expect(copyS3ObjectStub.called).to.be.true;
    expect(deleteS3ObjectStub.callCount).to.be.greaterThan(1);
  });

  it("usa getCurrentMonday/getCurrentDate/getNextMonthDate se le env var non sono impostate", async () => {
    process.env.ATHENA_DATABASE_NAME = "testdb";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";

    await eventHandler.handleEvent({});

    expect(getCurrentMondayStub.calledOnce).to.be.true;
    expect(getCurrentDateStub.calledOnce).to.be.true;
    expect(getNextMonthDateStub.calledOnce).to.be.true;
  });

  it("usa SPECIFIC_DATE/SPECIFIC_DAILY_DATE/SPECIFIC_MONTHLY_DATE se le env var sono impostate", async () => {
    process.env.ATHENA_DATABASE_NAME = "testdb";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";
    process.env.SPECIFIC_DATE = "2024-01-01";
    process.env.SPECIFIC_DAILY_DATE = "2024-01-15";
    process.env.SPECIFIC_MONTHLY_DATE = "2024-01";

    await eventHandler.handleEvent({});

    expect(getCurrentMondayStub.called).to.be.false;
    expect(getCurrentDateStub.called).to.be.false;
    expect(getNextMonthDateStub.called).to.be.false;
  });

  it("gestisce queryExecution null (nessun risultato)", async () => {
    queryExecutionStub.resolves(null);

    process.env.ATHENA_DATABASE_NAME = "db";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.SPECIFIC_DATE = "2024-01-01";
    process.env.SPECIFIC_DAILY_DATE = "2024-01-15";
    process.env.SPECIFIC_MONTHLY_DATE = "2024-01";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";

    await eventHandler.handleEvent({});

    // Se queryExecution ritorna null, copyS3Object non dovrebbe essere chiamato
    expect(copyS3ObjectStub.notCalled).to.be.true;
  });

  it("MONTHLY: esegue MonthlyCommessa quando oggi è il giorno 25", async () => {
    // Fissa la data al 25 del mese per triggherare il branch MONTHLY
    const clock = sinon.useFakeTimers(new Date("2024-01-25T12:00:00.000Z").getTime());

    process.env.ATHENA_DATABASE_NAME = "testdb";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.SPECIFIC_DATE = "2024-01-01";
    process.env.SPECIFIC_DAILY_DATE = "2024-01-25";
    process.env.SPECIFIC_MONTHLY_DATE = "2024-01";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";

    await eventHandler.handleEvent({});

    // Con giorno 25, MonthlyCommessa viene eseguita
    expect(queryExecutionStub.called).to.be.true;
    expect(copyS3ObjectStub.called).to.be.true;

    clock.restore();
  });

  it("MONTHLY: non esegue MonthlyCommessa quando oggi non è il giorno 25", async () => {
    // Fissa la data a un giorno diverso dal 25
    const clock = sinon.useFakeTimers(new Date("2024-01-10T12:00:00.000Z").getTime());

    // queryExecution restituisce null così le query DAILY non producono side-effect
    queryExecutionStub.resolves(null);

    process.env.ATHENA_DATABASE_NAME = "testdb";
    process.env.MONITORING_BUCKET_NAME = "bucket-test";
    process.env.SPECIFIC_DATE = "2024-01-01";
    process.env.SPECIFIC_DAILY_DATE = "2024-01-10";
    process.env.SPECIFIC_MONTHLY_DATE = "2024-01";
    process.env.ATHENA_WORKGROUP_NAME = "workgroup-test";

    await eventHandler.handleEvent({});

    // MonthlyCommessa non deve aver prodotto una copia (giorno != 25)
    expect(copyS3ObjectStub.notCalled).to.be.true;

    clock.restore();
  });
});
