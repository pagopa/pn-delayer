const axios = require("axios");

async function retrieveTimelineElements(iun) {
  if (!iun) {
    throw new Error("iun is required");
  }

  const params = {
    confidentialInfoRequired: false,
    strongly: false,
    timelineId: "PREPARE_",
  };

  try {
    const response = await axios.get(
      `${process.env.TIMELINE_SERVICE_BASE_PATH}/timeline-service-private/timelines/${iun}/elements`,
      { params }
    );
    return response.data;

  } catch (error) {
    console.error("Errore durante la chiamata alla timeline", {
          iun,
          error: error.message
        });
    throw error;
  }
}

module.exports = { retrieveTimelineElements };