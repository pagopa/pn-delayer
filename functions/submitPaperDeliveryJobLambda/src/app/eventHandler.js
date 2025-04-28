const ssmParameter = require ("./lib/ssmParameter.js");
const batchFunction = require ("./lib/batchFunction.js");

exports.handleEvent = async () => {
    let submittedJobs = [];
    const jobsInProgress = await batchFunction.listJobsByStatus();
    if (jobsInProgress.length > 0) {
       console.log(`Sono presenti ${jobsInProgress.length} Job in esecuzione o in attesa`);
       return submittedJobs;
    }

    const deliveryDriverProvinceMap = await ssmParameter.retrieveUnifiedDeliveryDriverProvince();
    const tuplesToSend = Object.entries(deliveryDriverProvinceMap).flatMap(([driver, provinces]) =>
      provinces.map((province) => [driver, province])
    ).map(tuple => tuple.join('~'));

    if(tuplesToSend.length === 0) {
        console.log("Nessuna coppia unifiedDeliveryDriver~Province trovata");
        return submittedJobs;
    }

    submittedJobs = await batchFunction.submitJobs(tuplesToSend);
    console.log(`Submit effettuata per ${submittedJobs.length} Job`);
    return submittedJobs;
};