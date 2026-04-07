"use strict";
const fs = require('fs');

const formatDate = (dateObj) => {
  const yyyy = String(dateObj.getFullYear());
  const mm = String(dateObj.getMonth() + 1).padStart(2, '0');
  const dd = String(dateObj.getDate()).padStart(2, '0');
  return { yyyy, mm, dd, full: `${yyyy}-${mm}-${dd}` };
};

function parseDateOrNull(dateStr) {
  if (!dateStr) {
    return null;
  }

  const date = new Date(dateStr);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid date: ${dateStr}`);
  }

  return date;
}

function prepareQueryCondition(queryFile, deliveryDate, executionDate, viewName) {
  if (!fs.existsSync(queryFile)) {
    throw new Error(`Query file not found: ${queryFile}`);
  }

  const map = prepareQueryPlaceholdersMap(deliveryDate, executionDate, viewName);

  let query = fs.readFileSync(queryFile, 'utf8');
  for (const [key, value] of Object.entries(map)) {
    //read query from queryFile and replace placeholders
    const placeholder = `<${key}>`;
    query = query.replace(new RegExp(placeholder, 'g'), value);
  }
  return query
}

function prepareQueryPlaceholdersMap(deliveryDate, executionDate, viewName) {
  const delivery = parseDateOrNull(deliveryDate);
  const execution = parseDateOrNull(executionDate);

  if (!delivery) {
    throw new Error("deliveryDate is required");
  }

  const nextWeek = new Date(delivery);
  const lastWeek = new Date(delivery);

  nextWeek.setDate(delivery.getDate() + 7);
  lastWeek.setDate(delivery.getDate() - 7);

  const deliveryFmt = formatDate(delivery);
  const executionFmt = execution ? formatDate(execution) : null;
  const nextFmt = formatDate(nextWeek);
  const lastFmt = formatDate(lastWeek);

  const referenceDate = executionFmt ?? deliveryFmt;

  const queryConditionQ1 = executionFmt
    ? `${generateExactPartitionCondition(
        executionFmt.full
      )} AND pk='${deliveryFmt.full}~EVALUATE_SENDER_LIMIT'`
    : `${generatePartitionConditionWithBetween(
        lastFmt.full,
        deliveryFmt.full
      )} AND pk='${deliveryFmt.full}~EVALUATE_SENDER_LIMIT'`;

  return {
    YYYY: referenceDate.yyyy,
    MM: referenceDate.mm,
    DD: referenceDate.dd,
    "YYYY-MM-DD-NEXT-WEEK": nextFmt.full,
    QUERY_CONDITION_Q1: queryConditionQ1,
    PAPER_DELIVERY_JSON_VIEW: viewName,
  };
}

function generateExactPartitionCondition(dateStr) {
  const date = new Date(dateStr);

  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid date: ${dateStr}`);
  }

  const { yyyy, mm, dd } = formatDate(date);

  return `(p_year = '${yyyy}' AND p_month = '${mm}' AND CAST(p_day AS INT) = ${Number(dd)})`;
}

function generatePartitionConditionWithBetween(startDateStr, endDateStr) {
  const start = new Date(startDateStr);
  const end = new Date(endDateStr);

  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    throw new Error(
      `Invalid date range: start=${startDateStr}, end=${endDateStr}`
    );
  }

  if (start > end) {
    throw new Error(
      "La data di inizio deve essere precedente o uguale alla data di fine"
    );
  }

  const result = [];
  const current = new Date(start);

  while (current <= end) {
    const y = current.getFullYear();
    const m = current.getMonth() + 1;
    const endOfMonth = new Date(y, m, 0);

    const fromDay =
      current.getFullYear() === start.getFullYear() &&
      current.getMonth() === start.getMonth()
        ? start.getDate()
        : 1;

    const toDay =
      current.getFullYear() === end.getFullYear() &&
      current.getMonth() === end.getMonth()
        ? end.getDate()
        : endOfMonth.getDate();

    result.push(
      `(p_year = '${y}' AND p_month = '${String(m).padStart(
        2,
        "0"
      )}' AND CAST(p_day AS INT) BETWEEN ${fromDay} AND ${toDay})`
    );

    current.setMonth(current.getMonth() + 1, 1);
  }

  return result.length === 1 ? result[0] : `(${result.join(" OR ")})`;
}

module.exports = {
  prepareQueryCondition,
  generatePartitionConditionWithBetween,
  generateExactPartitionCondition,
};