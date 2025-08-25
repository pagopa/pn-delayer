const { DynamoDBClient, QueryCommand } = require("@aws-sdk/client-dynamodb");
/*const { fromIni } = require("@aws-sdk/credential-provider-ini");

function awsClientCfg(profile) {
  const self = this;
  return {
    region: "eu-south-1",
    credentials: fromIni({
      profile: profile,
    })
  }
}*/

//const client = new DynamoDBClient(awsClientCfg('sso_pn-core-dev'));
const client = new DynamoDBClient();

async function queryRequestByIndex(tableName, indexName, key, value, lastEvaluatedKey) {
  const input = { // QueryInput
    TableName: tableName, // required
    IndexName: indexName,
    KeyConditionExpression: "#k = :k",
    ExpressionAttributeNames: { // ExpressionAttributeNameMap
      "#k": key,
    },
    ExpressionAttributeValues: {
      ":k": { "S": value }
    },
  };
  lastEvaluatedKey ? input['ExclusiveStartKey'] = lastEvaluatedKey : null
  const command = new QueryCommand(input);
  
  return await client.send(command);
}

module.exports = { queryRequestByIndex }