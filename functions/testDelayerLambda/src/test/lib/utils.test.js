const assert = require("assert");
const proxyquire = require("proxyquire").noCallThru();

describe("utils.js", () => {
  let existsSyncCalls, readFileSyncCalls, existsSyncReturn, readFileSyncReturn;
  let utils;

  beforeEach(() => {
    existsSyncCalls = [];
    readFileSyncCalls = [];
    existsSyncReturn = true;
    readFileSyncReturn = "";
    utils = proxyquire("../../app/lib/utils", {
      fs: {
        existsSync: (file) => {
          existsSyncCalls.push(file);
          return existsSyncReturn;
        },
        readFileSync: (file, enc) => {
          readFileSyncCalls.push([file, enc]);
          return readFileSyncReturn;
        }
      }
    });
  });

  it("prepareQueryCondition sostituisce correttamente i placeholder", () => {
    existsSyncReturn = true;
    readFileSyncReturn = "SELECT * FROM <PAPER_DELIVERY_JSON_VIEW> WHERE <QUERY_CONDITION_Q1>";
    const mdate = "2024-03-01";
    const database = "mydb";
    const query = utils.prepareQueryCondition("dummy.sql", mdate, database);
    assert.ok(query.includes("mydb"));
    assert.ok(query.includes("pk='"));
    assert.ok(query.includes("p_year"));
    assert.deepStrictEqual(existsSyncCalls, ["dummy.sql"]);
    assert.deepStrictEqual(readFileSyncCalls, [["dummy.sql", "utf8"]]);
  });

  it("prepareQueryCondition lancia errore se il file non esiste", () => {
    existsSyncReturn = false;
    assert.throws(() => utils.prepareQueryCondition("notfound.sql", "2024-03-01", "db"), /Query file not found/);
    assert.deepStrictEqual(existsSyncCalls, ["notfound.sql"]);
  });

  it("generatePartitionConditionWithBetween: stesso mese", () => {
    const { generatePartitionConditionWithBetween } = proxyquire("../../app/lib/utils", {});
    const cond = generatePartitionConditionWithBetween("2024-03-01", "2024-03-05");
    assert.ok(cond.includes("p_year = '2024'"));
    assert.ok(cond.includes("p_month = '03'"));
    assert.ok(cond.includes("BETWEEN 1 AND 5"));
  });

  it("generatePartitionConditionWithBetween: mesi diversi", () => {
    const { generatePartitionConditionWithBetween } = proxyquire("../../app/lib/utils", {});
    const cond = generatePartitionConditionWithBetween("2024-02-28", "2024-03-02");
    assert.ok(cond.includes("p_month = '02'"));
    assert.ok(cond.includes("p_month = '03'"));
    assert.ok(cond.includes("BETWEEN 28 AND 29"));
    assert.ok(cond.includes("BETWEEN 1 AND 2"));
  });

  it("generatePartitionConditionWithBetween lancia errore se la data di inizio è >= della data di fine", () => {
    const { generatePartitionConditionWithBetween } = proxyquire("../../app/lib/utils", {});
    assert.throws(() => generatePartitionConditionWithBetween("2024-03-05", "2024-03-01"), /La data di inizio deve essere precedente alla data di fine/);
    assert.throws(() => generatePartitionConditionWithBetween("2024-03-01", "2024-03-01"), /La data di inizio deve essere precedente alla data di fine/);
  });
});
