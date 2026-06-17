const { expect } = require("chai");
const sinon = require("sinon");
const fs = require("fs");

const {
  getCurrentMonday,
  getCurrentDate,
  getNextMonthDate,
  prepareQueryCondition
} = require("../app/lib/utils");

describe("Query Utils", () => {
  afterEach(() => {
    sinon.restore();
  });

  describe("getCurrentMonday()", () => {
    it("ritorna il lunedì corretto della settimana", () => {
      const fakeNow = new Date("2024-02-08T10:00:00Z"); // giovedì
      const realDate = Date;

      // Mockiamo la Date globale
      global.Date = class extends Date {
        constructor() {
          super();
          return fakeNow;
        }
        static now() {
          return fakeNow.getTime();
        }
      };

      const monday = getCurrentMonday();
      expect(monday).to.equal("2024-02-05");

      global.Date = realDate;
    });
  });

  describe("getCurrentDate()", () => {
    it("ritorna la data corrente nel formato YYYY-MM-DD", () => {
      const clock = sinon.useFakeTimers(new Date("2024-03-15T10:00:00Z").getTime());

      const result = getCurrentDate();
      expect(result).to.match(/^\d{4}-\d{2}-\d{2}$/);
      expect(result).to.equal("2024-03-15");

      clock.restore();
    });
  });

  describe("getNextMonthDate()", () => {
    it("ritorna l'anno e il mese successivo nel formato YYYY-MM", () => {
      const clock = sinon.useFakeTimers(new Date("2024-03-15T10:00:00Z").getTime());

      const result = getNextMonthDate();
      expect(result).to.match(/^\d{4}-\d{2}$/);
      expect(result).to.equal("2024-04");

      clock.restore();
    });

    it("gestisce correttamente il passaggio da dicembre a gennaio", () => {
      const clock = sinon.useFakeTimers(new Date("2024-12-10T10:00:00Z").getTime());

      const result = getNextMonthDate();
      expect(result).to.equal("2025-01");

      clock.restore();
    });
  });

  describe("prepareQueryCondition()", () => {

    it("sostituisce correttamente i placeholder", () => {
      const sqlContent = `
        SELECT *
        FROM tab
        WHERE date = '<YYYY-MM-DD>'
        AND cond = '<QUERY_CONDITION_Q1>';
      `;

      sinon.stub(fs, "existsSync").returns(true);
      sinon.stub(fs, "readFileSync").returns(sqlContent);

      const result = prepareQueryCondition("fake.sql", "2024-02-05");

      expect(result).to.include("2024-02-05");
      expect(result).to.not.include("<YYYY-MM-DD>");
      expect(result).to.not.include("<QUERY_CONDITION_Q1>");
    });

    it("lancia errore se il file non esiste", () => {
      sinon.stub(fs, "existsSync").returns(false);

      expect(() =>
        prepareQueryCondition("missing.sql", "2024-02-05")
      ).to.throw(/Query file not found/);
    });

    it("genera correttamente QUERY_CONDITION_Q1 (between)", () => {
      sinon.stub(fs, "existsSync").returns(true);
      sinon.stub(fs, "readFileSync").returns("SELECT '<QUERY_CONDITION_Q1>'");

      const result = prepareQueryCondition("fake.sql", "2024-02-05");

      expect(result).to.match(/p_year = '2024'/);
      expect(result).to.match(/CAST\(p_day AS INT\) BETWEEN/);
    });

    it("genera correttamente QUERY_CONDITION_Q2 (months)", () => {
      process.env.QUERY_MONTHS = "2";

      sinon.stub(fs, "existsSync").returns(true);
      sinon.stub(fs, "readFileSync").returns("SELECT '<QUERY_CONDITION_Q2>'");

      const result = prepareQueryCondition("fake.sql", "2024-02-05");

      expect(result).to.match(/p_month =/);
      expect(result).to.match(/OR/);
    });
  });
});
