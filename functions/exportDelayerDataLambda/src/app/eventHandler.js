const {
  getCurrentMonday,
  getAllElements,
  prepareCsv,
} = require("./lib/utils");
const { queryRequestByIndex } = require("./lib/dynamo");
const { getpresigneUrlObject, putObject } = require("./lib/s3");
const { publishToSnsTopic } = require("./lib/sns");

const pdDriverUsedCapacitiesTableName = process.env.PAPER_DELIVERY_DRIVER_USED_TABLENAME;
const deliveryDateIndex = process.env.DELIVERY_DATE_INDEX;
const snsTopicName = process.env.SNS_TOPIC_NAME;
const monitoringBucket = process.env.MONITORING_BUCKET_NAME;

exports.handleEvent = async (event) => {
  console.log("Event received:", JSON.stringify(event));
  const mDate = getCurrentMonday();

  const fileName = `${mDate}_DeliveryDriverUsed.csv`;

  try {
    const res = await getAllElements(
      queryRequestByIndex,
      pdDriverUsedCapacitiesTableName,
      deliveryDateIndex,
      "deliveryDate",
      mDate
    );

    let text = ''
    let messageId = 'not enabled'

    if(res.length > 0) {
      const csv = prepareCsv(res);
      await putObject(monitoringBucket, `postalizzazione/${fileName}`, csv);
      const downloadUrl = await getpresigneUrlObject(monitoringBucket, `postalizzazione/${fileName}`);
      text = `Il tuo file è pronto. Puoi scaricarlo qui ${downloadUrl}`
    }
    else {
      text = `Non ci sono informazioni per la data ${mDate}.`
    }
    
    if(snsTopicName) {
      console.log(text)
      messageId = await publishToSnsTopic(snsTopicName, text, `CSV Delivery Used Capacities ${mDate}`)
    }

    return {
      statusCode: 200,
      body: `Paper delivery driver used capacities for date ${mDate} exported successfully (SNSMessageID ${messageId})`,
    };
    
  } catch (error) {
    console.error(`Problem to retrieve paper delivery driver used capacities for date ${mDate}`, error);
    throw error;
  }

};
