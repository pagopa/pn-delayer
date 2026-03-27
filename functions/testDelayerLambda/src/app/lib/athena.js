const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } = require("@aws-sdk/client-athena");

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

  if (!response.QueryExecutionId) {
    throw new Error("Athena did not return a QueryExecutionId");
  }

  return response.QueryExecutionId;
}

async function getQueryExecution(queryExecutionId) {
  const input = {
    QueryExecutionId: queryExecutionId
  };

  const command = new GetQueryExecutionCommand(input);
  const response = await client.send(command);

  if (!response.QueryExecution) {
    throw new Error(`No QueryExecution found for id ${queryExecutionId}`);
  }

  return response.QueryExecution;
}

async function queryExecution(workgroup, query, database, outputLocation) {
  const queryExecutionId = await startQueryExecution(workgroup, query, database, outputLocation);
  console.log(`Athena query started. QueryExecutionId: ${queryExecutionId}`);

  while (true) {
    const queryExecution = await getQueryExecution(queryExecutionId);
    const status = queryExecution.Status?.State;
    const reason = queryExecution.Status?.StateChangeReason;
    const athenaError = queryExecution.Status?.AthenaError;
    const resultPath = queryExecution.ResultConfiguration?.OutputLocation;

    if (status === "SUCCEEDED") {
      if (!resultPath) {
        throw new Error(
          `Query succeeded but no OutputLocation was returned for QueryExecutionId=${queryExecutionId}`
        );
      }

      console.log(`Query result available at: ${resultPath}`);
      return resultPath;
    }

    if (status === "FAILED" || status === "CANCELLED") {
      console.error(
        "Athena query failed:",
        JSON.stringify({queryExecutionId, status, reason, athenaError}, null,2)
      );

      throw new Error(
        `Query execution failed with status=${status}` +
          `${reason ? `, reason=${reason}` : ""}` +
          `${athenaError ? `, athenaError=${JSON.stringify(athenaError)}` : ""}`
      );
    }
    await new Promise(resolve => setTimeout(resolve, 3000)); // wait for 3 seconds before checking again
  }
}

module.exports = { queryExecution };