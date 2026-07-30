const { expect } = require("chai");
const sinon = require("sinon");
const proxyquire = require("proxyquire").noCallThru().noPreserveCache();

describe("eventHandler.handleEvent", () => {
  let extractKinesisDataStub;
  let batchWritePaperDeliveryRecordsStub;
  let updateExcludeCounterStub;
  let updateSenderPriorityCounterStub;
  let batchWriteKinesisEventRecordsStub;
  let batchGetKinesisEventRecordsStub;
  let getSenderLimitStub;
  let updateUsedSenderLimitAndInsertPaperDeliveriesStub;
  let buildPaperDeliveryKinesisEventRecordStub;
  let groupRecordsByProductAndProvinceStub;
  let groupRecordsBySenderPaIdStub;
  let groupDelayedRecordsStub;
  let calculateNotificationSentAtWeekStub;
  let getDeliveryWeekStub;
  let getCurrentWeekStub;
  let addPaperDeliveryRecordStub;
  let lambda;

  beforeEach(() => {
    process.env.REGION = "us-east-1";
    process.env.KINESIS_PAPERDELIVERY_TABLE = "TestIncomingTable";
    process.env.KINESIS_PAPERDELIVERY_EVENTTABLE = "PaperDeliveryKinesisEventTable";
    process.env.KINESIS_PAPERDELIVERY_COUNTERTABLE = "TestCounterTable";
    process.env.KINESIS_PAPERDELIVERY_DELIVERYDATEDAYOFWEEK = "1";
    process.env.KINESIS_EVENTSRECORDTTLSECONDS = "3600";

    extractKinesisDataStub = sinon.stub();
    batchWritePaperDeliveryRecordsStub = sinon.stub();
    updateExcludeCounterStub = sinon.stub();
    updateSenderPriorityCounterStub = sinon.stub();
    batchWriteKinesisEventRecordsStub = sinon.stub();
    batchGetKinesisEventRecordsStub = sinon.stub();
    getSenderLimitStub = sinon.stub();
    updateUsedSenderLimitAndInsertPaperDeliveriesStub = sinon.stub();
    buildPaperDeliveryKinesisEventRecordStub = sinon.stub();
    groupRecordsByProductAndProvinceStub = sinon.stub();
    groupRecordsBySenderPaIdStub = sinon.stub();
    groupDelayedRecordsStub = sinon.stub();
    calculateNotificationSentAtWeekStub = sinon.stub();
    getDeliveryWeekStub = sinon.stub();
    getCurrentWeekStub = sinon.stub();
    addPaperDeliveryRecordStub = sinon.stub();

    getDeliveryWeekStub.returns("2026-07-27");
    getCurrentWeekStub.returns("2026-07-20");
    calculateNotificationSentAtWeekStub.returns("2026-07-20");

    addPaperDeliveryRecordStub.callsFake(({
      eventItem,
      deliveryWeek,
      delayed,
      skipSenderLimit,
      requestIds,
      paperDeliveryRecords
    }) => {
      if (requestIds.has(eventItem.requestId)) {
        return;
      }

      requestIds.add(eventItem.requestId);
      paperDeliveryRecords.push({
        entity: mockBuiltRecord(eventItem, {
          deliveryDate: deliveryWeek,
          delayed,
          skipSenderLimit
        }),
        kinesisSeqNumber: eventItem.kinesisSeqNumber
      });
    });

    groupDelayedRecordsStub.callsFake(records =>
      records.reduce((acc, record) => {
        const key = `${record.notificationSentAtWeek}~${record.senderPaId}~${record.productType}~${record.recipientNormalizedAddress.pr}`;
        if (!acc[key]) {
          acc[key] = [];
        }
        acc[key].push(record);
        return acc;
      }, {})
    );

    groupRecordsByProductAndProvinceStub.callsFake(records =>
      records.reduce((acc, record) => {
        const key = `${record.entity.province}~${record.entity.productType}`;
        if (!acc[key]) {
          acc[key] = [];
        }
        acc[key].push(record);
        return acc;
      }, {})
    );

    groupRecordsBySenderPaIdStub.callsFake(records =>
      records.reduce((acc, record) => {
        const key = record.entity.senderPaId;
        if (!key) {
          return acc;
        }
        if (!acc[key]) {
          acc[key] = [];
        }
        acc[key].push(record);
        return acc;
      }, {})
    );

    updateExcludeCounterStub.callsFake(async (_, failures) => failures);
    updateSenderPriorityCounterStub.callsFake(async (_, failures) => failures);
    batchWritePaperDeliveryRecordsStub.callsFake(async (_, failures) => failures);
    batchGetKinesisEventRecordsStub.resolves([]);
    buildPaperDeliveryKinesisEventRecordStub.callsFake(requestId => ({
      requestId,
      ttl: 9999999999
    }));
    batchWriteKinesisEventRecordsStub.resolves({});

    lambda = proxyquire("../app/eventHandler.js", {
      "./lib/kinesis": {
        extractKinesisData: extractKinesisDataStub
      },
      "./lib/dynamo": {
        batchWritePaperDeliveryRecords: batchWritePaperDeliveryRecordsStub,
        updateExcludeCounter: updateExcludeCounterStub,
        updateSenderPriorityCounter: updateSenderPriorityCounterStub,
        batchWriteKinesisEventRecords: batchWriteKinesisEventRecordsStub,
        batchGetKinesisEventRecords: batchGetKinesisEventRecordsStub,
        getSenderLimit: getSenderLimitStub,
        updateUsedSenderLimitAndInsertPaperDeliveries: updateUsedSenderLimitAndInsertPaperDeliveriesStub
      },
      "./lib/utils": {
        buildPaperDeliveryKinesisEventRecord: buildPaperDeliveryKinesisEventRecordStub,
        groupRecordsByProductAndProvince: groupRecordsByProductAndProvinceStub,
        groupRecordsBySenderPaId: groupRecordsBySenderPaIdStub,
        groupDelayedRecords: groupDelayedRecordsStub,
        calculateNotificationSentAtWeek: calculateNotificationSentAtWeekStub,
        getDeliveryWeek: getDeliveryWeekStub,
        getCurrentWeek: getCurrentWeekStub,
        addPaperDeliveryRecord: addPaperDeliveryRecordStub
      }
    });
  });

  afterEach(() => {
    sinon.restore();
  });

  function mockEvent(overrides = {}) {
    return {
      kinesisSeqNumber: "1234567890",
      unifiedDeliveryDriver: "driver1",
      recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region1" },
      requestId: "request1",
      productType: "AR",
      senderPaId: "sender1",
      tenderId: "tender1",
      iun: "iun1",
      notificationSentAt: "2026-07-21T00:00:00Z",
      prepareRequestDate: "2026-07-21T00:00:00Z",
      attempt: "0",
      senderPriority: 30,
      ...overrides
    };
  }

  function mockBuiltRecord(eventItem, overrides = {}) {
    return {
      pk: "2026-07-27~EVALUATE_SENDER_LIMIT",
      sk: `${eventItem.recipientNormalizedAddress.pr}~${eventItem.notificationSentAt}~${eventItem.requestId}`,
      requestId: eventItem.requestId,
      productType: eventItem.productType,
      senderPaId: eventItem.senderPaId,
      province: eventItem.recipientNormalizedAddress.pr,
      attempt: eventItem.attempt,
      communicationType: eventItem.communicationType || "LEGAL",
      delayed: false,
      skipSenderLimit: false,
      ...overrides
    };
  }

  it("should handle empty event data", async () => {
    extractKinesisDataStub.returns([]);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(batchGetKinesisEventRecordsStub.called).to.equal(false);
    expect(updateExcludeCounterStub.called).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should return no failures when all records are invalid", async () => {
    extractKinesisDataStub.returns([
      mockEvent({
        kinesisSeqNumber: "1",
        requestId: "req-1",
        notificationSentAt: undefined
      }),
      mockEvent({
        kinesisSeqNumber: "2",
        requestId: "req-2",
        attempt: undefined
      })
    ]);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(addPaperDeliveryRecordStub.called).to.equal(false);
    expect(batchGetKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should handle valid current week events", async () => {
    const eventData = [
      mockEvent(),
      mockEvent({
        kinesisSeqNumber: "1234567891",
        requestId: "request2",
        senderPaId: "sender2"
      })
    ];

    extractKinesisDataStub.returns(eventData);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(addPaperDeliveryRecordStub.callCount).to.equal(2);
    expect(getSenderLimitStub.called).to.equal(false);
    expect(batchGetKinesisEventRecordsStub.calledOnce).to.equal(true);
    expect(updateExcludeCounterStub.calledOnce).to.equal(true);
    expect(updateSenderPriorityCounterStub.calledOnce).to.equal(true);
    expect(batchWritePaperDeliveryRecordsStub.calledOnce).to.equal(true);
    expect(batchWriteKinesisEventRecordsStub.calledOnce).to.equal(true);
  });

  it("should skip already processed events", async () => {
    const eventData = [
      mockEvent(),
      mockEvent({
        kinesisSeqNumber: "1234567891",
        requestId: "request2",
        senderPaId: "sender2"
      })
    ];

    extractKinesisDataStub.returns(eventData);
    batchGetKinesisEventRecordsStub.resolves(["request1"]);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(batchWriteKinesisEventRecordsStub.calledOnce).to.equal(true);

    const eventRecordsArg = batchWriteKinesisEventRecordsStub.firstCall.args[0];
    expect(eventRecordsArg).to.deep.equal([
      { requestId: "request2", ttl: 9999999999 }
    ]);
  });

  it("should skip all already processed events", async () => {
    const eventData = [
      mockEvent(),
      mockEvent({
        kinesisSeqNumber: "1234567891",
        requestId: "request2",
        senderPaId: "sender2"
      })
    ];

    extractKinesisDataStub.returns(eventData);
    batchGetKinesisEventRecordsStub.resolves(["request1", "request2"]);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(updateExcludeCounterStub.called).to.equal(false);
    expect(updateSenderPriorityCounterStub.called).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should return failures when checking already processed events fails", async () => {
    const eventData = [
      mockEvent(),
      mockEvent({
        kinesisSeqNumber: "1234567891",
        requestId: "request2"
      })
    ];

    extractKinesisDataStub.returns(eventData);
    batchGetKinesisEventRecordsStub.rejects(new Error("DynamoDB error"));

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({
      batchItemFailures: [
        { itemIdentifier: "1234567890" },
        { itemIdentifier: "1234567891" }
      ]
    });
    expect(updateExcludeCounterStub.called).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should return failure when counter update fails", async () => {
    const eventData = [mockEvent()];

    extractKinesisDataStub.returns(eventData);
    updateExcludeCounterStub.resolves([
      { itemIdentifier: "1234567890" }
    ]);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({
      batchItemFailures: [
        { itemIdentifier: "1234567890" }
      ]
    });
    expect(updateSenderPriorityCounterStub.called).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should return failure when incoming batch write fails", async () => {
    const eventData = [mockEvent()];

    extractKinesisDataStub.returns(eventData);
    batchWritePaperDeliveryRecordsStub.resolves([
      { itemIdentifier: "1234567890" }
    ]);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({
      batchItemFailures: [
        { itemIdentifier: "1234567890" }
      ]
    });
    expect(updateSenderPriorityCounterStub.calledOnce).to.equal(true);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should return failure when writing Kinesis event records fails", async () => {
    const eventData = [mockEvent()];

    extractKinesisDataStub.returns(eventData);
    batchWriteKinesisEventRecordsStub.rejects(new Error("DynamoDB error"));

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({
      batchItemFailures: [
        { itemIdentifier: "1234567890" }
      ]
    });
  });

  it("should process only unique requestIds", async () => {
    const duplicated = mockEvent();

    extractKinesisDataStub.returns([
      duplicated,
      {
        ...duplicated,
        kinesisSeqNumber: "9999999999"
      }
    ]);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });

    const groupedArg = groupRecordsByProductAndProvinceStub.firstCall.args[0];
    expect(groupedArg).to.have.lengthOf(1);
    expect(groupedArg[0].entity.requestId).to.equal("request1");

    const groupedArgPaId = groupRecordsBySenderPaIdStub.firstCall.args[0];
    expect(groupedArgPaId).to.have.lengthOf(1);
    expect(groupedArgPaId[0].entity.requestId).to.equal("request1");
  });

  it("should skip records without attempt instead of failing them", async () => {
    const eventData = [
      mockEvent(),
      mockEvent({
        kinesisSeqNumber: "1234567891",
        requestId: "request2",
        attempt: undefined
      })
    ];

    extractKinesisDataStub.returns(eventData);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });

    const groupedArg = groupRecordsByProductAndProvinceStub.firstCall.args[0];
    expect(groupedArg).to.have.lengthOf(1);
    expect(groupedArg[0].kinesisSeqNumber).to.equal("1234567890");
  });

  it("should add delayed records to normal flow when sender limit does not exist", async () => {
    const delayedEvent = mockEvent({
      notificationSentAt: "2025-05-21T12:34:25Z",
      prepareRequestDate: "2025-05-21T12:34:25Z"
    });

    extractKinesisDataStub.returns([delayedEvent]);
    calculateNotificationSentAtWeekStub.returns("2025-05-19");
    getSenderLimitStub.resolves(null);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(getSenderLimitStub.calledOnceWithExactly(
      "sender1",
      "AR",
      "RM",
      "2025-05-19"
    )).to.equal(true);
    expect(updateUsedSenderLimitAndInsertPaperDeliveriesStub.called).to.equal(false);

    const delayedCall = addPaperDeliveryRecordStub.lastCall.args[0];
    expect(delayedCall.delayed).to.equal(true);
    expect(delayedCall.skipSenderLimit).to.equal(false);

    const writeRecords = batchWritePaperDeliveryRecordsStub.firstCall.args[0];
    expect(writeRecords).to.have.lengthOf(1);
    expect(writeRecords[0].entity.delayed).to.equal(true);
    expect(writeRecords[0].entity.skipSenderLimit).to.equal(false);
  });

  it("should process delayed records transactionally when sender limit exists", async () => {
    const delayedEvent = mockEvent({
      notificationSentAt: "2025-05-21T12:34:25Z",
      prepareRequestDate: "2025-05-21T12:34:25Z"
    });

    extractKinesisDataStub.returns([delayedEvent]);
    calculateNotificationSentAtWeekStub.returns("2025-05-19");
    getSenderLimitStub.resolves({ weeklyEstimate: 10 });

    updateUsedSenderLimitAndInsertPaperDeliveriesStub.callsFake(async (
      groupRecords,
      paperDeliveryRecords
    ) => {
      paperDeliveryRecords.push({
        entity: mockBuiltRecord(groupRecords[0], {
          delayed: true,
          skipSenderLimit: true
        }),
        kinesisSeqNumber: groupRecords[0].kinesisSeqNumber
      });
    });

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(updateUsedSenderLimitAndInsertPaperDeliveriesStub.calledOnceWithExactly(
      [sinon.match.object],
      sinon.match.array,
      "2025-05-19",
      10
    )).to.equal(true);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
    expect(batchWriteKinesisEventRecordsStub.calledOnce).to.equal(true);

    const counterRecords = groupRecordsByProductAndProvinceStub.firstCall.args[0];
    expect(counterRecords[0].entity.delayed).to.equal(true);
    expect(counterRecords[0].entity.skipSenderLimit).to.equal(true);
  });

  it("should insert delayed records normally when sender limit condition is not satisfied", async () => {
    const delayedEvent = mockEvent({
      notificationSentAt: "2025-05-21T12:34:25Z",
      prepareRequestDate: "2025-05-21T12:34:25Z"
    });

    extractKinesisDataStub.returns([delayedEvent]);
    calculateNotificationSentAtWeekStub.returns("2025-05-19");
    getSenderLimitStub.resolves({ weeklyEstimate: 1 });

    updateUsedSenderLimitAndInsertPaperDeliveriesStub.callsFake(async (
      groupRecords,
      paperDeliveryRecords
    ) => {
      paperDeliveryRecords.push({
        entity: mockBuiltRecord(groupRecords[0], {
          delayed: true,
          skipSenderLimit: false
        }),
        kinesisSeqNumber: groupRecords[0].kinesisSeqNumber
      });
    });

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(updateUsedSenderLimitAndInsertPaperDeliveriesStub.calledOnce).to.equal(true);
    expect(batchWritePaperDeliveryRecordsStub.calledOnce).to.equal(true);

    const writeRecords = batchWritePaperDeliveryRecordsStub.firstCall.args[0];
    expect(writeRecords).to.have.lengthOf(1);
    expect(writeRecords[0].entity.delayed).to.equal(true);
    expect(writeRecords[0].entity.skipSenderLimit).to.equal(false);
  });

  it("should treat zero weekly estimate as missing sender limit", async () => {
    const delayedEvent = mockEvent({
      notificationSentAt: "2025-05-21T12:34:25Z"
    });

    extractKinesisDataStub.returns([delayedEvent]);
    calculateNotificationSentAtWeekStub.returns("2025-05-19");
    getSenderLimitStub.resolves({ weeklyEstimate: 0 });

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(updateUsedSenderLimitAndInsertPaperDeliveriesStub.called).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.calledOnce).to.equal(true);

    const writeRecords = batchWritePaperDeliveryRecordsStub.firstCall.args[0];
    expect(writeRecords[0].entity.skipSenderLimit).to.equal(false);
  });

  it("should add delayed group records to failures when sender limit lookup fails", async () => {
    const delayedEvent = mockEvent({
      notificationSentAt: "2025-05-21T12:34:25Z"
    });

    extractKinesisDataStub.returns([delayedEvent]);
    calculateNotificationSentAtWeekStub.returns("2025-05-19");
    getSenderLimitStub.rejects(new Error("DynamoDB error"));

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({
      batchItemFailures: [
        { itemIdentifier: "1234567890" }
      ]
    });
    expect(batchGetKinesisEventRecordsStub.called).to.equal(false);
    expect(updateExcludeCounterStub.called).to.equal(false);
    expect(updateSenderPriorityCounterStub.called).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should add delayed group records to failures when transactional processing fails", async () => {
    const delayedEvent = mockEvent({
      notificationSentAt: "2025-05-21T12:34:25Z"
    });

    extractKinesisDataStub.returns([delayedEvent]);
    calculateNotificationSentAtWeekStub.returns("2025-05-19");
    getSenderLimitStub.resolves({ weeklyEstimate: 10 });
    updateUsedSenderLimitAndInsertPaperDeliveriesStub.rejects(
      new Error("DynamoDB error")
    );

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({
      batchItemFailures: [
        { itemIdentifier: "1234567890" }
      ]
    });
    expect(batchGetKinesisEventRecordsStub.called).to.equal(false);
    expect(updateExcludeCounterStub.called).to.equal(false);
    expect(updateSenderPriorityCounterStub.called).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should process delayed groups independently", async () => {
    const firstEvent = mockEvent({
      kinesisSeqNumber: "1",
      requestId: "request1",
      senderPaId: "sender1",
      recipientNormalizedAddress: { pr: "RM", cap: "00100" },
      notificationSentAt: "2025-05-21T12:34:25Z"
    });

    const secondEvent = mockEvent({
      kinesisSeqNumber: "2",
      requestId: "request2",
      senderPaId: "sender2",
      recipientNormalizedAddress: { pr: "MI", cap: "20100" },
      notificationSentAt: "2025-05-21T12:34:25Z"
    });

    extractKinesisDataStub.returns([firstEvent, secondEvent]);
    calculateNotificationSentAtWeekStub.returns("2025-05-19");

    getSenderLimitStub
      .withArgs("sender1", "AR", "RM", "2025-05-19")
      .rejects(new Error("DynamoDB error"));

    getSenderLimitStub
      .withArgs("sender2", "AR", "MI", "2025-05-19")
      .resolves(null);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({
      batchItemFailures: [
        { itemIdentifier: "1" }
      ]
    });
    expect(batchWritePaperDeliveryRecordsStub.calledOnce).to.equal(true);

    const writeRecords = batchWritePaperDeliveryRecordsStub.firstCall.args[0];
    expect(writeRecords).to.have.lengthOf(1);
    expect(writeRecords[0].entity.requestId).to.equal("request2");
  });
});