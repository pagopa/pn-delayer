const fs = require('fs');

function getCurrentMonday() {
  const now = new Date();
  const day = now.getDay(); 
  const diff = now.getDate() - day + (day === 0 ? -6 : 1); 
  const d = new Date(now.setDate(diff))
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

const formatDate = (dateObj) => {
  const yyyy = String(dateObj.getFullYear());
  const mm = String(dateObj.getMonth() + 1).padStart(2, '0');
  const dd = String(dateObj.getDate()).padStart(2, '0');
  return { yyyy, mm, dd, full: `${yyyy}-${mm}-${dd}` };
};

function prepareQueryCondition(queryFile, mdate) {
  if (!fs.existsSync(queryFile)) {
    throw new Error(`Query file not found: ${queryFile}`);
  }
  
  const map = prepareQueryPlaceholdersMap(mdate);

  let query = fs.readFileSync(queryFile, 'utf8');
  for (const [key, value] of Object.entries(map)) {
    //read query from queryFile and replace placeholders
    const placeholder = `<${key}>`;
    query = query.replace(new RegExp(placeholder, 'g'), value);
  }
  return query
}

function prepareQueryPlaceholdersMap(mDate) {
  const months = process.env.QUERY_MONTHS ? parseInt(process.env.QUERY_MONTHS) : 1;
  const baseDate = new Date(mDate);
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
    'YYYY-MM-DD': base.full,

    'YYYY-NEXT-WEEK': next.yyyy,
    'MM-NEXT-WEEK': next.mm,
    'DD-NEXT-WEEK': next.dd,
    'YYYY-MM-DD-NEXT-WEEK': next.full,

    'YYYY-LAST-WEEK': last.yyyy,
    'MM-LAST-WEEK': last.mm,
    'DD-LAST-WEEK': last.dd,
    
    'YYYY-MM-DD-LAST-WEEK': last.full,
    QUERY_CONDITION_Q1: `${generatePartitionConditionWithBetween(last.full, base.full)} AND pk='${base.full}~EVALUATE_SENDER_LIMIT'`,
    QUERY_CONDITION_Q2: generatePartitionConditionWithMonths(last.full, months),
    QUERY_CONDITION_Q3: `${generatePartitionConditionWithBetween(base.full, next.full)} AND pk='${base.full}~SENT_TO_PREPARE_PHASE_2'`,
  };
}

function generatePartitionConditionWithBetween(startDateStr, endDateStr) {
  const start = new Date(startDateStr);
  const end = new Date(endDateStr);

  if (start >= end) {
    throw new Error("La data di inizio deve essere precedente alla data di fine");
  }

  end.setDate(end.getDate());

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

function generatePartitionConditionWithMonths(dateStr, months) {
  const [y, m, d] = dateStr.split("-").map(Number);
  const end = new Date(y, m - 1, d);

  const start = new Date(end);
  start.setMonth(start.getMonth() - months);

  const result = [];
  const current = new Date(start);

  while (current <= end) {
    const yCurr = current.getFullYear();
    const mCurr = current.getMonth() + 1;

    result.push(`(p_year = '${yCurr}' AND p_month = '${String(mCurr).padStart(2, "0")}')`);

    current.setMonth(current.getMonth() + 1, 1);
  }

  return result.length === 1 ? result[0] : `(${result.join(" OR ")})`;
}



module.exports = { getCurrentMonday, prepareQueryCondition };