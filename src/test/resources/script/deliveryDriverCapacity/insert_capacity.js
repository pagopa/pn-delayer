const { parseArgs } = require('util');
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { AwsAuthClient } = require('./AwsAuthClient');
const { AwsDynamoDBClient } = require('./AwsDynamoDBClient');

const options = {
  options: {
    cicdProfile: { type: 'string', short: 'c' },
    coreProfile: { type: 'string', short: 'p' },
    fileName: { type: 'string', short: 'f' },
    tenderId: { type: 'string', short: 't' },
    local: { type: 'boolean', short: 'l', default: false },
    clearTable: { type: 'boolean', short: 'r', default: false }
  }
};

function current_date_iso() {
  return new Date().toISOString();
}

async function main() {

  console.time('csv-to-dynamo-processing');

  const parsedArgs = parseArgs(options);
  if (!parsedArgs.values.cicdProfile ||
    !parsedArgs.values.coreProfile ||
    !parsedArgs.values.fileName ||
    !parsedArgs.values.tenderId) {
    console.error('Missing required arguments. Usage:');
    console.error('node insert_capacity.js --cicdProfile <cicdProfile> --coreProfile <coreProfile> --fileName <nome del file csv> --tenderId <id della gara>');
    process.exit(1);
  }

  const awsAuthClient = new AwsAuthClient();

  const coreTemporaryCredentials = await awsAuthClient.ssoCredentials(
    parsedArgs.values.coreProfile,
    parsedArgs.values.local
  );

  const awsDynamoDBClient = new AwsDynamoDBClient(
    coreTemporaryCredentials,
    parsedArgs.values.local
  );

  const tenderId = parsedArgs.values.tenderId;
  const fileName = parsedArgs.values.fileName;

  if(parsedArgs.values.clearTable && parsedArgs.values.local) {
    console.log('clear table pn-PaperDeliveryDriverCapacities before insert');
    await awsDynamoDBClient.clearTable('pn-PaperDeliveryDriverCapacities');
  }

  console.log(`Processing CSV file: ${fileName} for tender ID: ${tenderId}`);

  try {
    const fileContent = fs.readFileSync(fileName, 'utf8');
    const records = parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      delimiter: ';'
    });

    console.log(`Found ${records.length} records to process`);


    const tableName = 'pn-PaperDeliveryDriverCapacities';

    const mappedItems = records.map(record => mappedItem(record, tenderId));
    console.log(`Mapped ${mappedItems.length} records`);
    await awsDynamoDBClient.groupAndBatchWriteItems(tableName, mappedItems);

    console.log('All records have been processed successfully');
  } catch (error) {
    console.error('Error processing CSV file:', error);
    process.exit(1);
  }

  console.timeEnd('csv-to-dynamo-processing');
}

function mappedItem(record, tenderId) {
  const current_date = current_date_iso();
  const date_from = new Date(new Date(current_date) - 86400000).toISOString();

  const pk = `${tenderId}~${record.unifiedDeliveryDriver}~${record.geoKey}`;

  return  {
        "pk": {"S": pk},
        "unifiedDeliveryDriver": {"S": record.unifiedDeliveryDriver},
        "geoKey": {"S": record.geoKey},
        "tenderId": {"S": tenderId},
        "capacity": {"N": record.capacity},
        "peakCapacity": {"N": record.peakCapacity},
        "createdAt": {"S": current_date},
        "activationDateFrom": {"S": record.activationDateFrom || date_from}
    }
}


main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});