const ssmParameter = require ("./lib/ssmParameter.js");
const batchFunction = require ("./lib/batchFunction.js");

exports.handleEvent = async () => {
    const now = new Date().toISOString();
    console.log(`Start Submit Job on ${now}`);
    const compactDate = now.slice(0,16).replace(/\D/g, '');
    let submittedJobs = [];
    const jobsInProgress = await batchFunction.listJobsByStatus();
    if (jobsInProgress) {
       console.log(`Sono presenti Job in esecuzione o in attesa`);
       return submittedJobs;
    }

    const deliveryDriverProvinceMap = await ssmParameter.retrieveUnifiedDeliveryDriverProvince();

    if(!deliveryDriverProvinceMap) {
        console.log("Nessuna coppia unifiedDeliveryDriver~Province trovata");
        return submittedJobs;
    }

    submittedJobs = await batchFunction.submitJobs(deliveryDriverProvinceMap, compactDate);
    console.log(`Submit effettuata per ${submittedJobs.length} Job`);
    return submittedJobs;
};