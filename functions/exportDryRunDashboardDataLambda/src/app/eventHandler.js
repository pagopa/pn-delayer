const { getCurrentMonday, prepareQueryCondition, getCurrentDate, getNextMonthDate } = require("./lib/utils");
const { queryExecution } = require("./lib/athena");
const { deleteS3Object, copyS3Object } = require("./lib/s3");

async function prepareQuery(fileName, date) {
  console.log(`Preparing query "${fileName}" for date: ${date}`);
  const query = prepareQueryCondition(`./resources/${fileName}.sql`, date);
  console.log(`Query "${fileName}" prepared.`);
  return query;
}

function extractKeyFromS3Path(bucket, s3Path) {
  return s3Path.replace(`s3://${bucket}/`, "");
}

async function moveAthenaResult(bucket, athenaResultPath, targetKey) {
  const resultKey = extractKeyFromS3Path(bucket, athenaResultPath);


  await copyS3Object(bucket, athenaResultPath.replace(`s3://`, ""), targetKey);
  await deleteS3Object(bucket, resultKey);
  await deleteS3Object(bucket, `${resultKey}.metadata`);

  console.log(`Moved Athena output to: s3://${bucket}/${targetKey}`);
}


async function prepareAndRunQuery(workgroup, name, sql, database, bucket, basePath, date) {
  console.log(`Executing query: ${name}`);

  const athenaOutputPath = `s3://${bucket}/${basePath}`;
  const result = await queryExecution(workgroup, sql, database, athenaOutputPath);

  if (!result) {
    console.warn(`No result for query ${name}, skipping.`);
    return;
  }

  const targetKey = `${basePath}/${name}/${date}.csv`;
  await moveAthenaResult(bucket, result, targetKey);
}


async function runAllQueries(workgroup, queries, database, bucket, basePath) {
  console.log("Preparing to execute queries:");
  for (const [name, sql] of Object.entries(queries)) {
    console.log(`- ${name}\n`);
    console.log(sql.query)
    switch (sql.cron.type) {
      case "DAILY":
        await prepareAndRunQuery(workgroup, name, sql.query, database, bucket, basePath, sql.outputName);
        break;
      case "WEEKLY":
        if (new Date().getDay() === sql.cron.day) {
          await prepareAndRunQuery(workgroup, name, sql.query, database, bucket, basePath, sql.outputName);
        }
        break;
      case "MONTHLY":
        if (new Date().getDate() === sql.cron.day) {
          await prepareAndRunQuery(workgroup, name, sql.query, database, bucket, basePath, sql.outputName);
        }
        break;
      default:
        console.warn(`Unknown cron type for query ${name}, skipping execution.`);
        //Execute query always if daily, the day of week if weekly and the day of month if monthly
        await prepareAndRunQuery(workgroup, name, sql.query, database, bucket, basePath, sql.outputName);

    }
    console.log("All queries executed successfully.");
  }
}

exports.handleEvent = async (event) => {
  console.log("Event received:", JSON.stringify(event));
  const specificDate = process.env.SPECIFIC_DATE || getCurrentMonday();
  const specificDailyDate = process.env.SPECIFIC_DAILY_DATE || getCurrentDate();
  const specificMonthlyDate = process.env.SPECIFIC_MONTHLY_DATE || getNextMonthDate();
  const database = process.env.ATHENA_DATABASE_NAME;
  const monitoringBucketName = process.env.MONITORING_BUCKET_NAME;
  const workgroupName = process.env.ATHENA_WORKGROUP_NAME;
  const basePath = `athena_results/data`;

  console.log(`Environment:
  - Date: ${specificDate}
  - Database: ${database}
  - Bucket: ${monitoringBucketName}
  - Output Path: s3://${monitoringBucketName}/${basePath}
  `);

  const queries = {
    SettimanaleEnteProvinciaProdotto: {
      query: await prepareQuery(
        "SettimanaleEnteProvinciaProdotto",
        specificDate
      ),
      outputName: specificDate,
      cron: {
        type: "DAILY",
      },
      SettimanaleRecapitista: {
        query: await prepareQuery(
          "SettimanaleRecapitista",
          specificDate
        ),
        outputName: specificDate,
        cron: {
          type: "DAILY",
        },
      },
      DailySpedizioniEnte: {
        query: await prepareQuery(
          "DailySpedizioniEnte",
          specificDailyDate
        ),
        outputName: specificDailyDate,
        cron: {
          type: "DAILY",
        },
      },
      MonthlyCommessa: {
        query: await prepareQuery(
          "MonthlyCommessa",
          specificMonthlyDate
        ),
        outputName: specificMonthlyDate,
        cron: {
          type: "MONTHLY",
          day: 25 //Max 25th of the month to avoid issues with months with less than 31 days
        },
      }
    }
  };
  await runAllQueries(workgroupName, queries, database, monitoringBucketName, basePath);
};
