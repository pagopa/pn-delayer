const { expect } = require("chai");
const { mockClient } = require("aws-sdk-client-mock");
const {
  SFNClient,
  ListExecutionsCommand
} = require("@aws-sdk/client-sfn");

const {
  Instant,
  LocalDate
} = require("@js-joda/core");

const { executionWithDeliveryDateExists } = require("../app/lib/stepFunction");

const sfnMock = mockClient(SFNClient);

process.env.RETRY_ALGORITHM_STATE_MACHINE = "prova";

describe("stepFunction", () => {
  const deliveryDate = LocalDate.parse("2025-01-20"); // Monday

  beforeEach(() => {
    sfnMock.reset();
  });

  it("should return false when an execution exists in the same ISO week", async () => {
    sfnMock.on(ListExecutionsCommand).resolves({
      executions: [
        {
          executionArn: "exec-1",
          startDate: Instant.parse("2025-01-21T10:00:00Z"), // same ISO week
        },
      ],
    });

    const result = await executionWithDeliveryDateExists(deliveryDate);

    expect(result).to.equal(false);
  });

  it("should return true when no executions exist", async () => {
    sfnMock.on(ListExecutionsCommand).resolves({
      executions: [],
    });

    const result = await executionWithDeliveryDateExists(deliveryDate);

    expect(result).to.equal(true);
  });
});
