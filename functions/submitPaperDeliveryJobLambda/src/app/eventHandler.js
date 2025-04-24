const { BatchClient, SubmitJobCommand, ListJobsCommand } = require ("@aws-sdk/client-batch");
const { retrieveDeliveryDriverIdProvince } = require ("./ssmParameter.js");

const batchClient = new BatchClient({ region: process.env.AWS_REGION });

exports.handleEvent = async () => {
  try {
    const jobQueue = process.env.JOB_QUEUE;
    const jobDefinition = process.env.JOB_DEFINITION;
    const jobName = process.env.JOB_NAME;

    const listJobsCommand = new ListJobsCommand({
      jobQueue,
      jobStatus: "RUNNING",
    });
    const runningJobs = await batchClient.send(listJobsCommand);

    if (runningJobs.jobSummaryList && runningJobs.jobSummaryList.length > 0) {
      console.log("Sono presenti Job in esecuzione o in attesa");
      return {
        statusCode: 200,
        body: JSON.stringify({
          message: "Sono presenti Job in esecuzione o in attesa",
        }),
      };
    }

    const deliveryDriverProvinceMap = await retrieveDeliveryDriverIdProvince(process.env.JOB_INPUT_PARAMETER);
    const tuples = Object.entries(deliveryDriverProvinceMap).flatMap(([driver, provinces]) =>
      provinces.map((province) => [driver, province])
    );

    for (const [driver, province] of tuples) {
      const unifiedDeliveryDriverProvince = `${driver}${province}`;

      const params = {
        jobDefinition,
        jobName,
        jobQueue,
        containerOverrides: {
          environment: [
            {
              name: "PN_DELAYER_UNIFIEDDELIVERYDRIVERPROVINCE",
              value: unifiedDeliveryDriverProvince,
            },
          ],
        },
      };

      const command = new SubmitJobCommand(params);
      const response = await batchClient.send(command);
      console.log(`Submit effettuata per la tupla ${driver}-${province}:`, response);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "",
      }),
    };
  } catch (error) {
    console.error("Errore durante la submit dei Job:", error);

    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Errore durante la submit dei Job",
        error: error.message,
      }),
    };
  }
};