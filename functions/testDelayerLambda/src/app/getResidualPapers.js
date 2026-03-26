const { prepareQueryCondition } = require("./lib/utils");
const { queryExecution } = require("./lib/athena");
const {
  convertAthenaCsvToSemicolonCsv,
  generatePresignedDownloadUrl,
  deleteS3Object
} = require("./lib/s3");

exports.getResidualPapers = async (params = []) => {
  const [paperDeliveryTableName, deliveryDate] = params;
  const specificDate = deliveryDate;
  const database = process.env.ATHENA_DATABASE_NAME;
  const bucket = process.env.BUCKET_NAME;
  const workgroup = process.env.ATHENA_WORKGROUP_NAME;
  const athenaResultsPrefix = `athena-results/residual-papers`;
  const finalPrefix = `residual-papers`;

  console.log(`Environment:
  - Date:      ${specificDate}
  - Database:  ${database}
  - Bucket:    ${bucket}
  - Workgroup: ${workgroup}`);

  const athenaOutputPath = `s3://${bucket}/${athenaResultsPrefix}/`;

  // Prepara SQL SELECT normale, senza UNLOAD
  const sql = await prepareQuery( "queryNotPassed", specificDate, paperDeliveryTableName);

  // Esegue la SELECT su Athena e ottiene il path del CSV standard Athena
  const athenaResultPath = await queryExecution(workgroup, sql, database, athenaOutputPath);

  if (!athenaResultPath) {
    console.warn(`No result for query "queryNotPassed", skipping file creation.`);
    return {
       message: 'No residual papers found for the given date.',
       downloadUrl: null,
       expiresIn: 0,
       key: null,
   };
  }

  const now = new Date().toISOString().replace(/\D/g, "").slice(0, 14);
  const targetKey = `${finalPrefix}/${now}.csv`;

  console.log(`Athena CSV result path: ${athenaResultPath}`);
  console.log(`Final target key: s3://${bucket}/${targetKey}`);

  // 4. Converte il CSV Athena da "," a ";" e salva il file finale
  await convertAthenaCsvToSemicolonCsv(bucket, athenaResultPath, targetKey);
  const sourceKey = athenaResultPath.replace("s3://", "");
  await deleteS3Object(bucket, sourceKey);
  await deleteS3Object(bucket, `${sourceKey}.metadata`);

  // 5. Presigned URL del file finale
  const downloadUrl = await generatePresignedDownloadUrl(bucket, targetKey);

  return {
    downloadUrl,
    expiresIn: 300,
    key: targetKey,
  };
};

async function prepareQuery(fileName, date, paperDeliveryTableName) {
  console.log(`Preparing query "${fileName}" for date: ${date}`);

  const query = prepareQueryCondition(
    `./resources/${fileName}.sql`,
    date,
    paperDeliveryTableName
  );

  console.log(`Query "${fileName}" prepared.`);
  return query;
}