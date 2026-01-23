const { executionWithDeliveryDateExists } = require("./lib/stepFunction");
const { getActiveScheduler } = require("./lib/eventBridge");
const { calculateDeliveryDate, normalizeToLocalDate } = require("./lib/utils");

exports.handleEvent = async (event = {}) => {
  const {
    deliveryDate,
    skipStepExecutionCheck = false,
    executionArn
  } = event;

  const finalDeliveryDate = normalizeToLocalDate(deliveryDate);
  let canExecuteRetryAlgorithm = true;

  if (!skipStepExecutionCheck){
    canExecuteRetryAlgorithm = await executionWithDeliveryDateExists(finalDeliveryDate, executionArn);
  }

  if (!canExecuteRetryAlgorithm) {
    return {
      executeRetryAlgorithm: false,
      schedulerName : null,
      schedulerExpression : null,
      deliveryDate: null
    };
  }
  const scheduler = await getActiveScheduler(finalDeliveryDate);

  return {
    executeRetryAlgorithm : canExecuteRetryAlgorithm,
    schedulerName : scheduler.name,
    schedulerExpression : scheduler.scheduleExpression,
    schedulerEndDate : scheduler.endDate,
    deliveryDate: finalDeliveryDate
  };
};
