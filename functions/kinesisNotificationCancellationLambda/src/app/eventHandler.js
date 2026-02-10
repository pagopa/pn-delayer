const { extractKinesisData } = require("./lib/kinesis.js");
const { executeTransactions, retrievePaperDelivery } = require("./lib/dynamo.js");
const { retrieveTimelineElements } = require("./lib/timelineClient.js");
const { ZoneId, LocalDate } = require("@js-joda/core");

exports.handleEvent = async (event) => {
    const cdcEvents = extractKinesisData(event);
    console.log(`Batch size: ${cdcEvents.length} cancellation requests`);

    if (cdcEvents.length === 0) {
        console.log("No events to process");
        return { batchItemFailures: [] };
    }

   const timelineElements = (
     await Promise.all(
       cdcEvents.map(async cdcEvent => {
           const elements = await retrieveTimelineElements(
             cdcEvent.dynamodb.NewImage.iun.S
           );

           return elements.map(el => ({
             ...el,
             kinesisSequenceNumber: cdcEvent.kinesisSequenceNumber
           }));
         })
     )
   ).flat();

    const filteredTimelineElements = timelineElements.filter(
      e =>
        e.category === "PREPARE_ANALOG_DOMICILE" || e.category === "PREPARE_SIMPLE_REGISTERED_LETTER"
    );

    const results = [];

    for (const element of filteredTimelineElements) {
         const paperDelivery = await retrievePaperDelivery(element.elementId);
         if(!paperDelivery) {
            console.warn(`No paper delivery found for element ${element.elementId}, skipping cancellation`);
            continue;
         }
         if (canCancel(paperDelivery)) {
           const result = await executeTransactions(
             [paperDelivery],
             element.kinesisSequenceNumber
           );
           results.push(result);
         }
    }

    const batchItemFailures = [
        ...new Set(
          results
            .filter(r => !r.success)
            .map(r => r.kinesisSequenceNumber)
          )
        ].map(seq => ({
        itemIdentifier: seq
    }));

    if (batchItemFailures.length > 0) {
      console.error(
        `Processing finished with ${batchItemFailures.length} failures`
      );
    } else {
      console.log("All cancellation requests processed successfully");
    }

    return { batchItemFailures };
  };

function canCancel(paperDelivery) {
  const dateString = paperDelivery?.pk?.split('~')[0];
  
  // check if dateString is a valid date in ISO format (YYYY-MM-DD)
  const datePattern = /^\d{4}-\d{2}-\d{2}$/;
  if (!datePattern.test(dateString)) {
    console.warn(`Invalid date format in pk: ${paperDelivery?.pk}`);
    return false;
  }
  // check if delivery date is today, if so, cancellation is not allowed because the delivery process may be in progress
  const nowIsDeliveryDate = LocalDate.parse(dateString).equals(LocalDate.now(ZoneId.UTC));
  const canCancelResult = paperDelivery?.workflowStep === "EVALUATE_SENDER_LIMIT" && !nowIsDeliveryDate;
  console.log(`Paper delivery with last workflow step ${paperDelivery?.workflowStep} and isSameDay ${nowIsDeliveryDate} canCancel: ${canCancelResult}`);
  return canCancelResult;
}