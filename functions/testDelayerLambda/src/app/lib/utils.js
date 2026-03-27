const fs = require('fs');

const formatDate = (dateObj) => {
  const yyyy = String(dateObj.getFullYear());
  const mm = String(dateObj.getMonth() + 1).padStart(2, '0');
  const dd = String(dateObj.getDate()).padStart(2, '0');
  return { yyyy, mm, dd, full: `${yyyy}-${mm}-${dd}` };
};

function prepareQueryCondition(queryFile, mdate, viewName) {
  if (!fs.existsSync(queryFile)) {
    throw new Error(`Query file not found: ${queryFile}`);
  }

  const map = prepareQueryPlaceholdersMap(mdate, viewName);

  let query = fs.readFileSync(queryFile, 'utf8');
  for (const [key, value] of Object.entries(map)) {
    //read query from queryFile and replace placeholders
    const placeholder = `<${key}>`;
    query = query.replace(new RegExp(placeholder, 'g'), value);
  }
  return query
}

function prepareQueryPlaceholdersMap(mDate, viewName) {
  const baseDate = new Date(mDate);

  if (Number.isNaN(baseDate.getTime())) {
    throw new Error(`Invalid date: ${mDate}`);
  }

  const nextWeek = new Date(baseDate);
  const lastWeek = new Date(baseDate);

  nextWeek.setDate(baseDate.getDate() + 7);
  lastWeek.setDate(baseDate.getDate() - 7);

  const base = formatDate(baseDate);
  const next = formatDate(nextWeek);
  const last = formatDate(lastWeek);

  return {
    YYYY: base.yyyy,
    MM: base.mm,
    DD: base.dd,
    'YYYY-MM-DD-NEXT-WEEK': next.full,
    QUERY_CONDITION_Q1: `${generatePartitionConditionWithBetween(last.full, base.full)} AND pk='${base.full}~EVALUATE_SENDER_LIMIT'`,
    PAPER_DELIVERY_JSON_VIEW : viewName
  };
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
    throw new Error("La data di inizio deve essere precedente o uguale alla data di fine");
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
      `(p_year = '${y}' AND p_month = '${String(m).padStart(2, "0")}' AND CAST(p_day AS INT) BETWEEN ${fromDay} AND ${toDay})`
    );

    current.setMonth(current.getMonth() + 1, 1);
  }
  return `${result.length === 1 ? result[0] : `(${result.join(" OR ")})`}`;
}

module.exports = { prepareQueryCondition, generatePartitionConditionWithBetween };
