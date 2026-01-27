const { executionWithCurrentDateExists } = require("./lib/stepFunction");
const { getActiveScheduler } = require("./lib/eventBridge");
const { calculateDeliveryDate, normalizeToLocalDate } = require("./lib/utils");
const { Instant, ZoneOffset, LocalDate } = require('@js-joda/core');

exports.handleEvent = async (event = {}) => {
  const {
    deliveryDate,
    skipStepExecutionCheck = false,
    executionArn
  } = event;

  const finalDeliveryDate = normalizeToLocalDate(deliveryDate);
  const currentDate = Instant.now().atZone(ZoneOffset.UTC).toLocalDate();
  let canExecuteRetryAlgorithm = true;

  if (!skipStepExecutionCheck){
    canExecuteRetryAlgorithm = await executionWithCurrentDateExists(currentDate, executionArn);
  }

  if (!canExecuteRetryAlgorithm) {
    return {
      executeRetryAlgorithm: false,
      schedulerName : null,
      schedulerExpression : null,
      schedulerEndDate : null,
      deliveryDate: null
    };
  }
  const scheduler = await getActiveScheduler(currentDate);

  return {
    executeRetryAlgorithm : canExecuteRetryAlgorithm,
    schedulerName : scheduler.name,
    schedulerExpression : scheduler.scheduleExpression,
    schedulerEndDate : scheduler.endDate,
    deliveryDate: finalDeliveryDate
  };
};
