const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } = require("@aws-sdk/client-athena");

const client = new AthenaClient();

async function startQueryExecution(queryString, database, outputLocation) {
  const input = {
    QueryString: queryString,
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

async function getQueryResults(queryExecutionId) {
  const input = {
    QueryExecutionId: queryExecutionId
  };

  const command = new GetQueryResultsCommand(input);
  const response = await client.send(command);
  return response.ResultSet;
}


async function queryExecution(query, database, outputLocation) {
  const result = await startQueryExecution(query, database, outputLocation);
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
    await new Promise(resolve => setTimeout(resolve, 5000)); // wait for 5 seconds before checking again
  }

  console.log(`Query result available`);
}

module.exports = { queryExecution }