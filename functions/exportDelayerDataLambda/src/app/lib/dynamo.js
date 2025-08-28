const { DynamoDBClient, QueryCommand } = require("@aws-sdk/client-dynamodb");

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