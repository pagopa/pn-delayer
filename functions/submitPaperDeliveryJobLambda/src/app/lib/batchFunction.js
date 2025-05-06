const { BatchClient, SubmitJobCommand, ListJobsCommand } = require ("@aws-sdk/client-batch");
const jobQueue = process.env.JOB_QUEUE;
const jobDefinition = process.env.JOB_DEFINITION;
const jobInputDriverEnvName = process.env.JOB_INPUT_DRIVER_ENV_NAME;
const jobInputProvinceListEnvName = process.env.JOB_INPUT_PROVINCE_LIST_ENV_NAME;

const batchClient = new BatchClient({ region: process.env.AWS_REGION });

async function listJobsByStatus() {
    const jobStatuses = ["RUNNING", "STARTING", "RUNNABLE", "PENDING", "SUBMITTED"];
    let foundJob = false;
    try {
        for (const status of jobStatuses) {
            const listJobsCommand = new ListJobsCommand({
                jobQueue: jobQueue,
                jobStatus: status,
            });
            const jobs = await batchClient.send(listJobsCommand);
            if (jobs.jobSummaryList && jobs.jobSummaryList.length > 0) {
                foundJob = true;
                break;
            }
        }
    } catch (error) {
        console.error("Errore durante il recupero dei Job:", error);
        throw error;
    }
    return foundJob;
}

async function submitJobs(deliveryDriverProvinceMap, compactDate) {
    let submittedJobs = [];
    Object.entries(JSON.parse(deliveryDriverProvinceMap))
        .flatMap(async ([driver, provinces]) => {
             const response = await submitJob(driver, provinces, compactDate);
             submittedJobs.push(`${response.jobId}#${driver}`);
             console.log(`Submitted job ${response.jobId} for unified delivery Driver: ${driver}`);
        });

    return submittedJobs;
}

async function submitJob(driver, provinces, compactDate){
    try {
        const jobName = `JOB_${driver}_${compactDate}`;
        const params = {
            jobDefinition,
            jobName,
            jobQueue,
            arrayProperties:{
                       size: provinces.length
                   },
            containerOverrides: {
             environment: [
               {
                 name: jobInputDriverEnvName,
                 value: driver
               },
               {
                 name: jobInputProvinceListEnvName,
                 value: provinces.join(",")
               }
             ],
            },
        };

        const command = new SubmitJobCommand(params);
        return await batchClient.send(command);
    } catch (error) {
       console.error("Errore durante la submit dei Job:", error);
       throw error;
    }
}

module.exports = { listJobsByStatus, submitJobs };
