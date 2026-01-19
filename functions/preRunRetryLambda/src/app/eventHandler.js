const { executionWithDeliveryDateExists } = require("./lib/stepFunction");
const { getActiveScheduler } = require("./lib/eventBridge");
const { calculateDeliveryDate } = require("./lib/utils");

exports.handleEvent = async (event = {}) => {
  const {
    skipStepExecutionCheck = false,
    executionArn
  } = event;

  const deliveryDate = calculateDeliveryDate();

  let canExecuteRetryAlgorithm = true;

  if (!skipStepExecutionCheck){
    canExecuteRetryAlgorithm = await executionWithDeliveryDateExists(deliveryDate, executionArn);
  }

  if (!canExecuteRetryAlgorithm) {
    return {
      executeRetryAlgorithm: false,
    };
  }

  const scheduler = await getActiveScheduler(deliveryDate);

  return {
    executeRetryAlgorithm : canExecuteRetryAlgorithm,
    schedulerName : scheduler.name,
    schedulerExpression : scheduler.expression,
    deliveryDate: deliveryDate
  };
};
