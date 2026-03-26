const { prepareQueryCondition } = require("./lib/utils");
const { queryExecution } = require("./lib/athena");
const { deleteS3Object, getS3Object, putS3Object, generatePresignedDownloadUrl } = require("./lib/s3");

const csv = require("csv-parser");
const { stringify } = require("csv-stringify");
const { PassThrough, pipeline } = require("stream");
const { promisify } = require("util");

const pipelineAsync = promisify(pipeline);

async function prepareQuery(fileName, date, paperDeliverJsonViewName) {
    console.log(`Preparing query "${fileName}" for date: ${date}`);
    const query = prepareQueryCondition(
        `./resources/${fileName}.sql`,
        date,
        paperDeliverJsonViewName
    );
    console.log(`Query "${fileName}" prepared.`);
    return query;
}

function extractKeyFromS3Path(bucket, s3Path) {
    return s3Path.replace(`s3://${bucket}/`, "");
}

async function convertAndUploadAthenaCsv(bucket, sourceKey, targetKey) {
  console.log(`Converting Athena CSV from s3://${bucket}/${sourceKey} to s3://${bucket}/${targetKey}`);

  const sourceObject = await getS3Object(bucket, sourceKey);

  if (!sourceObject) {
    throw new Error(`Empty body for source object: s3://${bucket}/${sourceKey}`);
  }

  let stringifierInitialized = false;
  const outputStream = new PassThrough();
  const uploadPromise = putS3Object(bucket, targetKey, outputStream, "text/csv; charset=utf-8");
  const parser = csv();

  parser.on("headers", (headers) => {
    if (!stringifierInitialized) {
      const stringifier = stringify({
        header: true,
        delimiter: ";",
        columns: headers,
      });

      stringifier.pipe(outputStream);
      parser.on("data", (row) => stringifier.write(row));
      parser.on("end", () => stringifier.end());
      parser.on("error", (err) => stringifier.destroy(err));

      stringifierInitialized = true;
    }
  });

  const parsePromise = new Promise((resolve, reject) => {
    parser.on("end", resolve);
    parser.on("error", reject);
    sourceObject.on("error", reject);
    sourceObject.pipe(parser);
  });

  await Promise.all([parsePromise, uploadPromise]);
  console.log(`Converted file uploaded to s3://${bucket}/${targetKey}`);
}

async function publishAthenaResult(bucket, athenaResultPath, targetKey) {
    const sourceKey = extractKeyFromS3Path(bucket, athenaResultPath);
    await convertAndUploadAthenaCsv(bucket, sourceKey, targetKey);
    await deleteS3Object(bucket, sourceKey);
    await deleteS3Object(bucket, `${sourceKey}.metadata`);
    console.log(`Published Athena output to: s3://${bucket}/${targetKey}`);
}

exports.getResidualPapers = async (params = []) => {
   const [paperDeliverJsonViewName, deliveryDate] = params;

    const specificDate = deliveryDate;
    const database     = process.env.ATHENA_DATABASE_NAME;
    const bucket       = process.env.BUCKET_NAME;
    const workgroup    = process.env.ATHENA_WORKGROUP_NAME;
    const basePath     = `residual-papers`;

    console.log(`Environment:
  - Date:      ${new Date(specificDate).getTime()}
  - Database:  ${database}
  - Bucket:    ${bucket}
  - Workgroup: ${workgroup}`);

    // 1. Prepara SQL con sostituzione placeholder
    const sql = await prepareQuery("queryNotPassed", specificDate, paperDeliverJsonViewName);

    // 2. Esegui la query su Athena → restituisce il path S3 del CSV prodotto
    const athenaOutputPath = `s3://${bucket}/${basePath}`;
    const athenaResultPath = await queryExecution(workgroup, sql, database, athenaOutputPath);

    if (!athenaResultPath) {
        console.warn(`No result for query "queryNotPassed", skipping file publish.`);
         return  {
            message: 'No residual papers found for the given date.',
            downloadUrl: null,
            expiresIn: 0,
            key: null,
        };
    }

    const now = new Date().toISOString().replace(/\D/g, '').slice(0, 14);
    const targetKey = `${basePath}/${now}.csv`;

    await publishAthenaResult(bucket, athenaResultPath, targetKey);

    const downloadUrl = await generatePresignedDownloadUrl(bucket, targetKey);
    return {
        downloadUrl,
        expiresIn: 300,
        key: targetKey,
    };
};
