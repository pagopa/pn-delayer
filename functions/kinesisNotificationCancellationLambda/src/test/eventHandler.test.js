const { expect } = require("chai");
const proxyquire = require("proxyquire").noPreserveCache();
const { LocalDate, ZoneId } = require("@js-joda/core");

describe("Cancellation Lambda Handler", () => {

  const lambda = proxyquire.noCallThru().load("../app/eventHandler.js", {
    "../app/lib/kinesis.js": {
      extractKinesisData: (event) => event.mockKinesisData || []
    },
    "../app/lib/timelineClient.js": {
      retrieveTimelineElements: async (iun) => []
    },
    "../app/lib/dynamo.js": {
      retrievePaperDelivery: async () => null,
      executeTransactions: async () => ({
        success: true,
        kinesisSequenceNumber: "seq"
      })
    }
  });

  beforeEach(() => {
    // reset manuale se serve in futuro
  });

  it("should return empty batchItemFailures when no events", async () => {
    const result = await lambda.handleEvent({ mockKinesisData: [] });
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });


  it("should process a valid cancellation successfully", async () => {
    const handler = proxyquire.noCallThru().load("../app/eventHandler.js", {
      "../app/lib/kinesis.js": {
        extractKinesisData: () => [
          {
            kinesisSequenceNumber: "seq-1",
            dynamodb: {
              NewImage: {
                category: "NOTIFICATION_CANCELLATION_REQUEST",
                iun: "IUN123"
              }
            }
          }
        ]
      },
      "../app/lib/timelineClient.js": {
        retrieveTimelineElements: async () => [
          {
              category: "PREPARE_ANALOG_DOMICILE",
              timelineElementId: "PREPARE_ANALOG_DOMICILE.IUN_IUN789.RECINDEX_0.ATTEMPT_0",
            },
            {
              category: "PREPARE_SIMPLE_REGISTERED_LETTER",
              timelineElementId: "PREPARE_SIMPLE_REGISTERED_LETTER.IUN_IUN789.RECINDEX_0.ATTEMPT_0",
            }
        ]
      },
      "../app/lib/dynamo.js": {
        retrievePaperDelivery: async () => ({
          workflowStep: "EVALUATE_SENDER_LIMIT",
          pk: "2026-01-19~SENT_TO_PREPARE_PHASE_2",
          sk: "SK"
        }),
        executeTransactions: async () => ({
          success: true,
          kinesisSequenceNumber: "seq-1"
        })
      }
    });

    const result = await handler.handleEvent({});
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

  it("should return batchItemFailures when transaction fails", async () => {
    const handler = proxyquire.noCallThru().load("../app/eventHandler.js", {
      "../app/lib/kinesis.js": {
        extractKinesisData: () => [
          {
            kinesisSequenceNumber: "seq-2",
            dynamodb: {
              NewImage: {
                category: "NOTIFICATION_CANCELLATION_REQUEST",
                iun: "IUN456"
              }
            }
          }
        ]
      },
      "../app/lib/timelineClient.js": {
        retrieveTimelineElements: async () => [
          {
              category: "PREPARE_ANALOG_DOMICILE",
              timelineElementId: "PREPARE_ANALOG_DOMICILE.IUN_IUN789.RECINDEX_0.ATTEMPT_0",
            },
            {
              category: "PREPARE_SIMPLE_REGISTERED_LETTER",
              timelineElementId: "PREPARE_SIMPLE_REGISTERED_LETTER.IUN_IUN789.RECINDEX_0.ATTEMPT_0",
            }
        ]
      },
      "../app/lib/dynamo.js": {
        retrievePaperDelivery: async () => ({
          workflowStep: "EVALUATE_SENDER_LIMIT",
          pk: "2026-01-19~SENT_TO_PREPARE_PHASE_2"
        }),
        executeTransactions: async () => ({
          success: false,
          kinesisSequenceNumber: "seq-2"
        })
      },
      "@js-joda/core": {
        LocalDate: {
          parse: () => ({
            equals: () => false
          }),
          now: () => ({})
        },
        ZoneId: { UTC: {} }
      }
    });

    const result = await handler.handleEvent({});

    expect(result).to.deep.equal({
      batchItemFailures: [{ itemIdentifier: "seq-2" }]
    });
  });

  it("should skip when workflowStep is not cancellable for workflowStep", async () => {
    const handler = proxyquire.noCallThru().load("../app/eventHandler.js", {
      "../app/lib/kinesis.js": {
        extractKinesisData: () => [
          {
            kinesisSequenceNumber: "seq-3",
            dynamodb: {
              NewImage: {
                category: "NOTIFICATION_CANCELLATION_REQUEST",
                iun: "IUN789"
              }
            }
          }
        ]
      },
      "../app/lib/timelineClient.js": {
        retrieveTimelineElements: async () => [
          {
            category: "PREPARE_ANALOG_DOMICILE",
            timelineElementId: "PREPARE_ANALOG_DOMICILE.IUN_IUN789.RECINDEX_0.ATTEMPT_0",
          },
          {
            category: "PREPARE_SIMPLE_REGISTERED_LETTER",
            timelineElementId: "PREPARE_SIMPLE_REGISTERED_LETTER.IUN_IUN789.RECINDEX_0.ATTEMPT_0",
          }
        ]
      },
      "../app/lib/dynamo.js": {
        retrievePaperDelivery: async () => ({
          workflowStep: "OTHER_STEP",
          pk: "2026-01-20~SENT_TO_PREPARE_PHASE_2"
        }),
        executeTransactions: async () => {
          throw new Error("Should not be called");
        }
      }
    });

    const result = await handler.handleEvent({});
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

  it("should skip when workflowStep is not cancellable for isSameDay", async () => {
    const handler = proxyquire.noCallThru().load("../app/eventHandler.js", {
      "../app/lib/kinesis.js": {
        extractKinesisData: () => [
          {
            kinesisSequenceNumber: "seq-3",
            dynamodb: {
              NewImage: {
                category: "NOTIFICATION_CANCELLATION_REQUEST",
                iun: "IUN789"
              }
            }
          }
        ]
      },
      "../app/lib/timelineClient.js": {
        retrieveTimelineElements: async () => [
          {
            category: "PREPARE_ANALOG_DOMICILE",
            timelineElementId: "PREPARE_ANALOG_DOMICILE.IUN_IUN789.RECINDEX_0.ATTEMPT_0",
          },
          {
            category: "PREPARE_SIMPLE_REGISTERED_LETTER",
            timelineElementId: "PREPARE_SIMPLE_REGISTERED_LETTER.IUN_IUN789.RECINDEX_0.ATTEMPT_0",
          }
        ]
      },
      "../app/lib/dynamo.js": {
        retrievePaperDelivery: async () => ({
          workflowStep: "EVALUATE_SENDER_LIMIT",
          pk: `${LocalDate.now(ZoneId.UTC).toString()}~SENT_TO_PREPARE_PHASE_2`
        }),
        executeTransactions: async () => {
          throw new Error("Should not be called");
        }
      }
    });

    const result = await handler.handleEvent({});
    expect(result).to.deep.equal({ batchItemFailures: [] });
  });

});
