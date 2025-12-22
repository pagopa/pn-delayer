const { expect } = require("chai");
const proxyquire = require("proxyquire").noPreserveCache();

describe("retrieveTimelineElements", () => {

  let axiosMock;
  let retrieveTimelineElements;

  beforeEach(() => {
     axiosMock = {
       get: async () => ({ data: [] })
     };

     const module = proxyquire
       .noCallThru()
       .load("../app/lib/timelineClient.js", {
         axios: axiosMock
       });

     retrieveTimelineElements = module.retrieveTimelineElements;
   });

  it("should throw error if iun is missing", async () => {
    try {
      await retrieveTimelineElements();
      throw new Error("Test should have failed");
    } catch (error) {
      expect(error.message).to.equal("iun is required");
    }
  });

  it("should call timeline service with correct URL and params", async () => {
    process.env.TIMELINE_SERVICE_BASE_PATH = "http://timeline-service";

    let calledUrl;
    let calledOptions;

    axiosMock.get = async (url, options) => {
      calledUrl = url;
      calledOptions = options;
      return { data: [{ id: "el1" }] };
    };

    const result = await retrieveTimelineElements("IUN123");

    expect(calledUrl).to.equal(
      "http://timeline-service/timeline-service-private/timelines/IUN123/elements"
    );

    expect(calledOptions).to.deep.equal({
      params: {
        confidentialInfoRequired: false,
        strongly: false,
        timelineId: "PREPARE_"
      }
    });

    expect(result).to.deep.equal([{ id: "el1" }]);
  });

  it("should return empty array when timeline is empty", async () => {
    axiosMock.get = async () => ({ data: [] });

    const result = await retrieveTimelineElements("IUN_EMPTY");
    expect(result).to.deep.equal([]);
  });

  it("should propagate axios error", async () => {
    axiosMock.get = async () => {
      throw new Error("Timeline service error");
    };

    try {
      await retrieveTimelineElements("IUN_FAIL");
      throw new Error("Test should have failed");
    } catch (error) {
      expect(error.message).to.equal("Timeline service error");
    }
  });

});
