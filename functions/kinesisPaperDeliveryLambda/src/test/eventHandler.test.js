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
  let buildPaperDeliveryRecordStub;
  let buildPaperDeliveryKinesisEventRecordStub;
  let groupRecordsByProductAndProvinceStub;
  let groupRecordsBySenderPaIdStub;
  let getDeliveryWeekStub;
  let getCurrentWeekStub;
  let calculateNotificationSentAtWeekStub;
  let groupDelayedRecordsStub;
  let getSenderLimitStub;
  let updateDelayedCounter;
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
    buildPaperDeliveryRecordStub = sinon.stub();
    buildPaperDeliveryKinesisEventRecordStub = sinon.stub();
    groupRecordsByProductAndProvinceStub = sinon.stub();
    groupRecordsBySenderPaIdStub =  sinon.stub();
    getDeliveryWeekStub = sinon.stub();
    getCurrentWeekStub = sinon.stub();
    calculateNotificationSentAtWeekStub = sinon.stub();
    groupDelayedRecordsStub = sinon.stub();
    getSenderLimitStub = sinon.stub();
    updateDelayedCounter = sinon.stub();

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
        updateDelayedCounter: updateDelayedCounter
      },
      "./lib/utils": {
        buildPaperDeliveryRecord: buildPaperDeliveryRecordStub,
        buildPaperDeliveryKinesisEventRecord: buildPaperDeliveryKinesisEventRecordStub,
        groupRecordsByProductAndProvince: groupRecordsByProductAndProvinceStub,
        groupRecordsBySenderPaId: groupRecordsBySenderPaIdStub,
        getDeliveryWeek: getDeliveryWeekStub,
        getCurrentWeek: getCurrentWeekStub,
        calculateNotificationSentAtWeek: calculateNotificationSentAtWeekStub,
        groupDelayedRecords: groupDelayedRecordsStub
      }
    });
  });

  afterEach(() => {
    sinon.restore();
  });

  function mockBuiltRecord(eventItem, overrides = {}) {
    return {
      pk: "2026-03-31~EVALUATE_SENDER_LIMIT",
      sk: `${eventItem.recipientNormalizedAddress.pr}~${eventItem.notificationSentAt}~${eventItem.requestId}`,
      requestId: eventItem.requestId,
      productType: eventItem.productType,
      province: eventItem.recipientNormalizedAddress.pr,
      attempt: eventItem.attempt,
      communicationType: eventItem.communicationType || "LEGAL",
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
      {
        kinesisSeqNumber: "1",
        requestId: "req-1",
        productType: "RS",
        recipientNormalizedAddress: { pr: "RM", cap: "12345" },
        prepareRequestDate: "2024-10-01T00:00:00Z"
        // notificationSentAt missing
      },
      {
        kinesisSeqNumber: "2",
        requestId: "req-2",
        productType: "RS",
        recipientNormalizedAddress: { pr: "MI", cap: "54321" },
        notificationSentAt: "2023-10-01T00:00:00Z"
        // attempt missing
      }
    ]);

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(batchGetKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should handle a valid Kinesis event", async () => {
    const eventData = [
      {
        kinesisSeqNumber: "1234567890",
        unifiedDeliveryDriver: "driver1",
        recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region1" },
        requestId: "request1",
        productType: "RS",
        senderPaId: "sender1",
        tenderId: "tender1",
        iun: "iun1",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0",
        senderPriority: 30
      },
      {
        kinesisSeqNumber: "1234567891",
        unifiedDeliveryDriver: "driver2",
        recipientNormalizedAddress: { pr: "RM", cap: "54321", region: "region2" },
        requestId: "request2",
        productType: "RS",
        senderPaId: "sender2",
        tenderId: "tender2",
        iun: "iun2",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0",
        senderPriority: 70
      }
    ];

    extractKinesisDataStub.returns(eventData);

    buildPaperDeliveryRecordStub.callsFake((item) => mockBuiltRecord(item));
    batchGetKinesisEventRecordsStub.resolves([]);
    groupRecordsByProductAndProvinceStub.returns({
      "RM~RS": [
        { entity: mockBuiltRecord(eventData[0]), kinesisSeqNumber: "1234567890" },
        { entity: mockBuiltRecord(eventData[1]), kinesisSeqNumber: "1234567891" }
      ]
    });
    groupRecordsBySenderPaIdStub.returns({
      "sender1": [
        { entity: mockBuiltRecord(eventData[0]), kinesisSeqNumber: "1234567890" }
      ],
      "sender2": [
        { entity: mockBuiltRecord(eventData[1]), kinesisSeqNumber: "1234567891" }
      ],
    });
    updateExcludeCounterStub.callsFake(async (_, failures) => failures);
    updateSenderPriorityCounterStub.callsFake(async (_, failures) => failures);
    batchWritePaperDeliveryRecordsStub.callsFake(async (_, failures) => failures);
    buildPaperDeliveryKinesisEventRecordStub.callsFake((requestId) => ({ requestId, ttl: 9999999999 }));
    batchWriteKinesisEventRecordsStub.resolves({ UnprocessedItems: {} });
    getCurrentWeekStub.returns('2026-07-20');
    calculateNotificationSentAtWeekStub.returns('2026-07-20');

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(batchGetKinesisEventRecordsStub.calledOnce).to.equal(true);
    expect(updateExcludeCounterStub.calledOnce).to.equal(true);
    expect(updateSenderPriorityCounterStub.calledOnce).to.equal(true);
    expect(batchWritePaperDeliveryRecordsStub.calledOnce).to.equal(true);
    expect(batchWriteKinesisEventRecordsStub.calledOnce).to.equal(true);
  });

  it("should skip already processed events", async () => {
    const eventData = [
      {
        kinesisSeqNumber: "1234567890",
        unifiedDeliveryDriver: "driver1",
        recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region2" },
        requestId: "request1",
        productType: "AR",
        senderPaId: "sender1",
        tenderId: "tender1",
        iun: "iun1",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0"
      },
      {
        kinesisSeqNumber: "1234567891",
        unifiedDeliveryDriver: "driver2",
        recipientNormalizedAddress: { pr: "RM", cap: "54321", region: "region2" },
        requestId: "request2",
        productType: "AR",
        senderPaId: "sender2",
        tenderId: "tender2",
        iun: "iun2",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0",
        senderPriority: 70
      }
    ];

    extractKinesisDataStub.returns(eventData);
    buildPaperDeliveryRecordStub.callsFake((item) => mockBuiltRecord(item));
    batchGetKinesisEventRecordsStub.resolves(["request1"]);
    groupRecordsByProductAndProvinceStub.returns({
      "RM~AR": [{ entity: mockBuiltRecord(eventData[1]), kinesisSeqNumber: "1234567891" }]
    });
    groupRecordsBySenderPaIdStub.returns({
      "sender2": [
        { entity: mockBuiltRecord(eventData[1]), kinesisSeqNumber: "1234567891" }
      ],
    });
    updateExcludeCounterStub.callsFake(async (_, failures) => failures);
    updateSenderPriorityCounterStub.callsFake(async (_, failures) => failures);
    batchWritePaperDeliveryRecordsStub.callsFake(async (_, failures) => failures);
    buildPaperDeliveryKinesisEventRecordStub.callsFake((requestId) => ({ requestId, ttl: 9999999999 }));
    batchWriteKinesisEventRecordsStub.resolves({});
    getCurrentWeekStub.returns('2026-07-20');
    calculateNotificationSentAtWeekStub.returns('2026-07-20');

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(batchGetKinesisEventRecordsStub.calledOnce).to.equal(true);
    expect(batchWriteKinesisEventRecordsStub.calledOnce).to.equal(true);
    expect(updateSenderPriorityCounterStub.calledOnce).to.equal(true);

    const eventRecordsArg = batchWriteKinesisEventRecordsStub.firstCall.args[0];
    expect(eventRecordsArg).to.deep.equal([{ requestId: "request2", ttl: 9999999999 }]);
  });

  it("should skip all already processed events", async () => {
    const eventData = [
      {
        kinesisSeqNumber: "1234567890",
        unifiedDeliveryDriver: "driver1",
        recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region2" },
        requestId: "1234567890",
        productType: "type1",
        senderPaId: "sender1",
        tenderId: "tender1",
        iun: "iun1",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0"
      },
      {
        kinesisSeqNumber: "1234567891",
        unifiedDeliveryDriver: "driver2",
        recipientNormalizedAddress: { pr: "RM", cap: "54321", region: "region2" },
        requestId: "1234567891",
        productType: "type2",
        senderPaId: "sender2",
        tenderId: "tender2",
        iun: "iun2",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0"
      }
    ];

    extractKinesisDataStub.returns(eventData);
    buildPaperDeliveryRecordStub.callsFake((item) => mockBuiltRecord(item));
    batchGetKinesisEventRecordsStub.resolves(["1234567890", "1234567891"]);
    getCurrentWeekStub.returns('2026-07-20');
    calculateNotificationSentAtWeekStub.returns('2026-07-20');

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });
    expect(updateExcludeCounterStub.called).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should return failure when counter update fails", async () => {
    const eventData = [
      {
        kinesisSeqNumber: "1234567890",
        unifiedDeliveryDriver: "driver1",
        recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region1" },
        requestId: "request1",
        productType: "RS",
        senderPaId: "sender1",
        tenderId: "tender1",
        iun: "iun1",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0"
      }
    ];

    extractKinesisDataStub.returns(eventData);
    buildPaperDeliveryRecordStub.callsFake((item) => mockBuiltRecord(item));
    batchGetKinesisEventRecordsStub.resolves([]);
    groupRecordsByProductAndProvinceStub.returns({
      "RM~RS": [{ entity: mockBuiltRecord(eventData[0]), kinesisSeqNumber: "1234567890" }]
    });
    updateExcludeCounterStub.resolves([{ itemIdentifier: "1234567890" }]);
    batchWritePaperDeliveryRecordsStub.callsFake(async (_, failures) => failures);
    getCurrentWeekStub.returns('2026-07-20');
    calculateNotificationSentAtWeekStub.returns('2026-07-20');

    const result = await lambda.handleEvent({});

    expect(result.batchItemFailures).to.deep.equal([{ itemIdentifier: "1234567890" }]);
    expect(updateSenderPriorityCounterStub.calledOnce).to.equal(false);
    expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
  });

  it("should return failure when incoming batch write fails", async () => {
    const eventData = [
      {
        kinesisSeqNumber: "1234567890",
        unifiedDeliveryDriver: "driver1",
        recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region1" },
        requestId: "request1",
        productType: "RS",
        senderPaId: "sender1",
        tenderId: "tender1",
        iun: "iun1",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0",
        senderPriority: 70
      }
    ];

    extractKinesisDataStub.returns(eventData);
    buildPaperDeliveryRecordStub.callsFake((item) => mockBuiltRecord(item));
    batchGetKinesisEventRecordsStub.resolves([]);
    groupRecordsByProductAndProvinceStub.returns({
      "RM~RS": [{ entity: mockBuiltRecord(eventData[0]), kinesisSeqNumber: "1234567890" }]
    });
    groupRecordsBySenderPaIdStub.returns({
      "sender1": [
        { entity: mockBuiltRecord(eventData[0]), kinesisSeqNumber: "1234567890" }
      ]
    });
    updateExcludeCounterStub.callsFake(async (_, failures) => failures);
    updateSenderPriorityCounterStub.callsFake(async (_, failures) => failures);
    batchWritePaperDeliveryRecordsStub.resolves([{ itemIdentifier: "1234567890" }]);
    getCurrentWeekStub.returns('2026-07-20');
    calculateNotificationSentAtWeekStub.returns('2026-07-20');

    const result = await lambda.handleEvent({});

    expect(result.batchItemFailures).to.deep.equal([{ itemIdentifier: "1234567890" }]);
    expect(updateSenderPriorityCounterStub.calledOnce).to.equal(true);
    expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
  });

  it("should process only unique requestIds", async () => {
    const duplicated = {
      kinesisSeqNumber: "1234567890",
      unifiedDeliveryDriver: "driver1",
      recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region1" },
      requestId: "request1",
      productType: "RS",
      senderPaId: "sender1",
      tenderId: "tender1",
      iun: "iun1",
      notificationSentAt: "2023-10-01T00:00:00Z",
      prepareRequestDate: "2024-10-01T00:00:00Z",
      attempt: "0",
      senderPriority: 40
    };

    extractKinesisDataStub.returns([
      duplicated,
      { ...duplicated, kinesisSeqNumber: "9999999999" }
    ]);

    buildPaperDeliveryRecordStub.callsFake((item) => mockBuiltRecord(item));
    batchGetKinesisEventRecordsStub.resolves([]);
    groupRecordsByProductAndProvinceStub.callsFake((records) => ({
      "RM~RS": records
    }));
    groupRecordsBySenderPaIdStub.callsFake((records) => ({
      "sender1": records
    }));
    updateExcludeCounterStub.callsFake(async (_, failures) => failures);
    updateSenderPriorityCounterStub.callsFake(async (_, failures) => failures);
    batchWritePaperDeliveryRecordsStub.callsFake(async (_, failures) => failures);
    buildPaperDeliveryKinesisEventRecordStub.callsFake((requestId) => ({ requestId, ttl: 9999999999 }));
    batchWriteKinesisEventRecordsStub.resolves({});
    getCurrentWeekStub.returns('2026-07-20');
    calculateNotificationSentAtWeekStub.returns('2026-07-20');

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
      {
        kinesisSeqNumber: "1234567890",
        unifiedDeliveryDriver: "driver1",
        recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region2" },
        requestId: "request1",
        productType: "AR",
        senderPaId: "sender1",
        tenderId: "tender1",
        iun: "iun1",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        attempt: "0",
        senderPriority: 40
      },
      {
        kinesisSeqNumber: "1234567891",
        unifiedDeliveryDriver: "driver2",
        recipientNormalizedAddress: { pr: "RM", cap: "54321", region: "region2" },
        requestId: "request2",
        productType: "AR",
        senderPaId: "sender2",
        tenderId: "tender2",
        iun: "iun2",
        notificationSentAt: "2023-10-01T00:00:00Z",
        prepareRequestDate: "2024-10-01T00:00:00Z",
        senderPriority: 15
        // attempt missing
      }
    ];

    extractKinesisDataStub.returns(eventData);
    buildPaperDeliveryRecordStub.callsFake((item) => mockBuiltRecord(item));
    batchGetKinesisEventRecordsStub.resolves([]);
    groupRecordsByProductAndProvinceStub.returns({
      "RM~AR": [{ entity: mockBuiltRecord(eventData[0]), kinesisSeqNumber: "1234567890" }]
    });
    groupRecordsBySenderPaIdStub.returns({
      "sender1": [
        { entity: mockBuiltRecord(eventData[0]), kinesisSeqNumber: "1234567890" }
      ]
    });
    updateExcludeCounterStub.callsFake(async (_, failures) => failures);
    updateSenderPriorityCounterStub.callsFake(async (_, failures) => failures);
    batchWritePaperDeliveryRecordsStub.callsFake(async (_, failures) => failures);
    buildPaperDeliveryKinesisEventRecordStub.callsFake((requestId) => ({ requestId, ttl: 9999999999 }));
    batchWriteKinesisEventRecordsStub.resolves({});
    getCurrentWeekStub.returns('2026-07-20');
    calculateNotificationSentAtWeekStub.returns('2026-07-20');

    const result = await lambda.handleEvent({});

    expect(result).to.deep.equal({ batchItemFailures: [] });

    const groupedArg = groupRecordsByProductAndProvinceStub.firstCall.args[0];
    expect(groupedArg).to.have.lengthOf(1);
    expect(groupedArg[0].kinesisSeqNumber).to.equal("1234567890");

    const groupedArgPaId = groupRecordsBySenderPaIdStub.firstCall.args[0];
    expect(groupedArgPaId).to.have.lengthOf(1);
    expect(groupedArgPaId[0].kinesisSeqNumber).to.equal("1234567890");
  });

  it("should mark delayed true and create delayed counter when record is not in current week and sender limit exists", async () => {
      const delayedEvent = {
        kinesisSeqNumber: "1234567890",
        unifiedDeliveryDriver: "driver1",
        recipientNormalizedAddress: { pr: "RM", cap: "12345", region: "region1" },
        requestId: "request1",
        productType: "AR",
        senderPaId: "sender1",
        tenderId: "tender1",
        iun: "iun1",
        notificationSentAt: "2025-05-21T12:34:25Z",
        prepareRequestDate: "2025-05-21T12:34:25Z",
        attempt: "0",
        senderPriority: 30
      };

      extractKinesisDataStub.returns([delayedEvent]);
      getDeliveryWeekStub.returns("2025-05-26");
      getCurrentWeekStub.returns('2026-07-20');
      calculateNotificationSentAtWeekStub.returns('2026-07-13');
      groupDelayedRecordsStub.returns({
        "2025-05-19~sender1~AR~RM": [delayedEvent]
      });
      getSenderLimitStub.resolves({ weeklyEstimate: 15 });
      updateDelayedCounter.resolves();
      buildPaperDeliveryRecordStub.callsFake((item, deliveryWeek, delayed, skipSenderLimit) =>
        mockBuiltRecord(item, { deliveryDate: deliveryWeek, delayed, skipSenderLimit })
      );
      batchGetKinesisEventRecordsStub.resolves([]);
      groupRecordsByProductAndProvinceStub.callsFake((records) => ({ "RM~AR": records }));
      groupRecordsBySenderPaIdStub.callsFake((records) => ({ sender1: records }));
      updateExcludeCounterStub.callsFake(async (_, failures) => failures);
      updateSenderPriorityCounterStub.callsFake(async (_, failures) => failures);
      batchWritePaperDeliveryRecordsStub.callsFake(async (_, failures) => failures);
      buildPaperDeliveryKinesisEventRecordStub.callsFake((requestId) => ({ requestId, ttl: 9999999999 }));
      batchWriteKinesisEventRecordsStub.resolves({});

      const result = await lambda.handleEvent({});

      expect(result).to.deep.equal({ batchItemFailures: [] });
      expect(getSenderLimitStub.calledOnceWithExactly("sender1", "AR", "RM", "2025-05-19")).to.equal(true);
      expect(updateDelayedCounter.calledOnceWithExactly("2025-05-26", "2025-05-19", "sender1", "AR", "RM", 1, 15)).to.equal(true);
      expect(buildPaperDeliveryRecordStub.firstCall.args[2]).to.equal(true);
      expect(batchWritePaperDeliveryRecordsStub.calledOnce).to.equal(true);
      expect(batchWriteKinesisEventRecordsStub.calledOnce).to.equal(true);
    });

    it("should not create delayed counter and should keep delayed false when sender limit is missing", async () => {
      const delayedEvent = {
        kinesisSeqNumber: "1234567891",
        unifiedDeliveryDriver: "driver2",
        recipientNormalizedAddress: { pr: "MI", cap: "54321", region: "region2" },
        requestId: "request2",
        productType: "AR",
        senderPaId: "sender2",
        tenderId: "tender2",
        iun: "iun2",
        notificationSentAt: "2025-05-21T12:34:25Z",
        prepareRequestDate: "2025-05-21T12:34:25Z",
        attempt: "1",
        senderPriority: 20
      };

      extractKinesisDataStub.returns([delayedEvent]);
      getDeliveryWeekStub.returns("2025-05-26");
      getCurrentWeekStub.returns('2026-07-20');
      calculateNotificationSentAtWeekStub.returns('2026-07-13');
      groupDelayedRecordsStub.returns({
        "2025-05-19~sender2~AR~MI": [delayedEvent]
      });
      getSenderLimitStub.resolves(null);
      buildPaperDeliveryRecordStub.callsFake((item, deliveryWeek, delayed, skipSenderLimit) =>
        mockBuiltRecord(item, { deliveryDate: deliveryWeek, delayed, skipSenderLimit })
      );
      batchGetKinesisEventRecordsStub.resolves([]);
      groupRecordsByProductAndProvinceStub.callsFake((records) => ({ "MI~AR": records }));
      groupRecordsBySenderPaIdStub.callsFake((records) => ({ sender2: records }));
      updateExcludeCounterStub.callsFake(async (_, failures) => failures);
      updateSenderPriorityCounterStub.callsFake(async (_, failures) => failures);
      batchWritePaperDeliveryRecordsStub.callsFake(async (_, failures) => failures);
      buildPaperDeliveryKinesisEventRecordStub.callsFake((requestId) => ({ requestId, ttl: 9999999999 }));
      batchWriteKinesisEventRecordsStub.resolves({});

      const result = await lambda.handleEvent({});

      expect(result).to.deep.equal({ batchItemFailures: [] });
      expect(getSenderLimitStub.calledOnceWithExactly("sender2", "AR", "MI", "2025-05-19")).to.equal(true);
      expect(updateDelayedCounter.called).to.equal(false);
      expect(buildPaperDeliveryRecordStub.firstCall.args[2]).to.equal(false);
      expect(batchWritePaperDeliveryRecordsStub.calledOnce).to.equal(true);
    });

    it("should add delayed records to failures when delayed counter creation fails", async () => {
      const delayedEvent = {
        kinesisSeqNumber: "1234567892",
        unifiedDeliveryDriver: "driver3",
        recipientNormalizedAddress: { pr: "TO", cap: "10100", region: "region3" },
        requestId: "request3",
        productType: "AR",
        senderPaId: "sender3",
        tenderId: "tender3",
        iun: "iun3",
        notificationSentAt: "2025-05-21T12:34:25Z",
        prepareRequestDate: "2025-05-21T12:34:25Z",
        attempt: "0",
        senderPriority: 10
      };

      extractKinesisDataStub.returns([delayedEvent]);
      getDeliveryWeekStub.returns("2025-05-26");
      getCurrentWeekStub.returns('2026-07-20');
      calculateNotificationSentAtWeekStub.returns('2026-07-13');
      groupDelayedRecordsStub.returns({
        "2025-05-19~sender3~AR~TO": [delayedEvent]
      });
      getSenderLimitStub.resolves({ weeklyEstimate: 8 });
      updateDelayedCounter.rejects(new Error("counter error"));

      const result = await lambda.handleEvent({});

      expect(result).to.deep.equal({ batchItemFailures: [{ itemIdentifier: "1234567892" }] });
      expect(buildPaperDeliveryRecordStub.called).to.equal(false);
      expect(batchGetKinesisEventRecordsStub.called).to.equal(false);
      expect(updateExcludeCounterStub.called).to.equal(false);
      expect(updateSenderPriorityCounterStub.called).to.equal(false);
      expect(batchWritePaperDeliveryRecordsStub.called).to.equal(false);
      expect(batchWriteKinesisEventRecordsStub.called).to.equal(false);
    });

});