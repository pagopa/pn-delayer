const { BatchClient, SubmitJobCommand, ListJobsCommand } = require ("@aws-sdk/client-batch");
const jobQueue = process.env.JOB_QUEUE;
const jobDefinition = process.env.JOB_DEFINITION;
const jobInputEnvName = process.env.JOB_INPUT_ENV_NAME;

const batchClient = new BatchClient({ region: process.env.AWS_REGION });

async function listJobsByStatus() {
    const jobStatuses = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"];
    let jobsInProgress = [];
    try {
        for (const status of jobStatuses) {
            const listJobsCommand = new ListJobsCommand({
                jobQueue,
                jobStatus: status,
            });
            const jobs = await batchClient.send(listJobsCommand);
            if (jobs.jobSummaryList && jobs.jobSummaryList.length > 0) {
              jobsInProgress = jobsInProgress.concat(jobs.jobSummaryList);
            }
        }
    } catch (error) {
        console.error("Errore durante il recupero dei Job:", error);
        throw error;
    }
    return jobsInProgress;
}

async function submitJobs(tuples){
    const jobName = "JOB_" + new Date().toISOString();
    let submittedJobs = [];
    try {
        for (const unifiedDeliveryDriverProvince of tuples) {
          const params = {
            jobDefinition,
            jobName,
            jobQueue,
            containerOverrides: {
              environment: [
                {
                  name: jobInputEnvName,
                  value: unifiedDeliveryDriverProvince,
                },
              ],
            },
          };

          const command = new SubmitJobCommand(params);
          const response = await batchClient.send(command);
          submittedJobs.push(`${response.jobId}#${unifiedDeliveryDriverProvince}`);
          console.log(`Submitted job ${response.jobId} for tuple: ${unifiedDeliveryDriverProvince}`);
        }
        return submittedJobs;
    } catch (error) {
        console.error("Errore durante la submit dei Job:", error);
        throw error;
    }
}

module.exports = { listJobsByStatus, submitJobs };
