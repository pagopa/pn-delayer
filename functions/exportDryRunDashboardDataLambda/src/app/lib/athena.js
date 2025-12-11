const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } = require("@aws-sdk/client-athena");

const client = new AthenaClient();

async function startQueryExecution(workgroup, queryString, database, outputLocation) {
  const input = {
    QueryString: queryString,
    WorkGroup: workgroup,
    QueryExecutionContext: {
      Database: database
    },
    ResultConfiguration: {
      OutputLocation: outputLocation,
    }
  };
  
  const command = new StartQueryExecutionCommand(input);
  const response = await client.send(command);
  return response.QueryExecutionId;
}

async function getQueryExecution(queryExecutionId) {
  const input = {
    QueryExecutionId: queryExecutionId
  };

  const command = new GetQueryExecutionCommand(input);
  const response = await client.send(command);
  return response.QueryExecution;
}

async function queryExecution(workgroup, query, database, outputLocation) {
  const result = await startQueryExecution(workgroup, query, database, outputLocation);
  let fileResult;
  while (true) {
    const queryExecution = await getQueryExecution(result);
    const status = queryExecution.Status.State;
    fileResult = queryExecution.ResultConfiguration.OutputLocation;
    console.log(`Query execution status: ${status}`);
    if (status === 'SUCCEEDED') {
      break;
    } else if (status === 'FAILED' || status === 'CANCELLED') {
      throw new Error(`Query execution failed with status: ${status}`);
    }
    await new Promise(resolve => setTimeout(resolve, 3000)); // wait for 5 seconds before checking again
  }

  console.log(`Query result available`);
  return fileResult;
}

module.exports = { queryExecution }