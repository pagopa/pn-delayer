const fs = require('fs');

function getCurrentMonday() {
  const now = new Date();
  const day = now.getDay(); 
  const diff = now.getDate() - day + (day === 0 ? -6 : 1); 
  const d = new Date(now.setDate(diff))
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

async function getAllElements(func, ...args) {
  let nextToken = undefined;
  let first = true
  let elements = []
  while(first || nextToken) {
    first = false;
    console.log(...args)
    let result = await func(...args, nextToken)
    nextToken = result.nextToken
    elements = elements.concat(result.Items)
  }
  return elements
}

function extractValue(field) {
  return field?.S || field?.N || '';
}

function prepareCsv(data) {
  
  const headers = Object.keys(data[0]);
  const rows = data.map(row =>
    headers.map(header => extractValue(row[header])).join(',')
  );

  const csv = [headers.join(','), ...rows].join('\n');
  return csv
}

module.exports = { getCurrentMonday, getAllElements, prepareCsv};