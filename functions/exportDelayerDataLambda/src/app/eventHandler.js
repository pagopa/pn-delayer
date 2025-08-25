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
/*const pdDriverUsedCapacitiesTableName = "pn-PaperDeliveryDriverUsedCapacities";
const deliveryDateIndex = "deliveryDate-index";
const snsTopicName = "arn:aws:sns:eu-south-1:830192246553:pn-SnsToEmail";
*/
exports.handleEvent = async (event) => {
  console.log("Event received:", JSON.stringify(event));
  const mDate = getCurrentMonday();

  const fileName = `${mDate}.csv`;

  try {
    res = await getAllElements(
      queryRequestByIndex,
      pdDriverUsedCapacitiesTableName,
      deliveryDateIndex,
      "deliveryDate",
      mDate
    );

    const csv = prepareCsv(res);
    
    await putObject(monitoringBucket, `postalizzazione/${fileName}`, csv);
    const downloadUrl = await getpresigneUrlObject(monitoringBucket, `postalizzazione/${fileName}`);
    if(snsTopicName) {
      const text = `Il tuo file è pronto. Puoi scaricarlo qui ${downloadUrl}`
      console.log(text)
      await publishToSnsTopic(snsTopicName, text, 'capacity delivery')
    }

    return {
      statusCode: 200,
      body: `Paper delivery driver used capacities for date ${mDate} exported successfully`,
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      body: `Problem to retrieve paper delivery driver used capacities for date ${mDate}`,
    };
  }

};
