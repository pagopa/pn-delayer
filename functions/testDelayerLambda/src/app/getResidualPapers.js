const { getCurrentMonday, prepareQueryCondition } = require("./lib/utils");
const { queryExecution } = require("./lib/athena");
const { deleteS3Object, copyS3Object, getS3Object, putS3Object, generatePresignedDownloadUrl } = require("./lib/s3");
const path = require("path");

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

function extractKeyFromS3Path(bucket, s3Path) {
    return s3Path.replace(`s3://${bucket}/`, "");
}

async function convertCsvSeparator(bucket, key) {
    const content = await getS3Object(bucket, key);
    const converted = content.split("\n").map(row => row.replace(/,/g, ";")).join("\n");
    await putS3Object(bucket, key, converted);
}

async function moveAthenaResult(bucket, athenaResultPath, targetKey) {
    const resultKey = extractKeyFromS3Path(bucket, athenaResultPath);
    await copyS3Object(bucket, athenaResultPath.replace(`s3://`, ""), targetKey);
    await deleteS3Object(bucket, resultKey);
    await deleteS3Object(bucket, `${resultKey}.metadata`);
    console.log(`Moved Athena output to: s3://${bucket}/${targetKey}`);
}

exports.getResidualPapers = async (params = []) => {
   const [paperDeliveryTableName, deliveryDate] = params;

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
    const sql = await prepareQuery("queryNotPassed", specificDate, paperDeliveryTableName);

    // 2. Esegui la query su Athena → restituisce il path S3 del CSV prodotto
    const athenaOutputPath = `s3://${bucket}/${basePath}`;
    const athenaResultPath = await queryExecution(workgroup, sql, database, athenaOutputPath);

    if (!athenaResultPath) {
        console.warn(`No result for query "queryNotPassed", skipping file move.`);
        return;
    }

    const now = new Date().toISOString().replace(/\D/g, '').slice(0, 14);
    const targetKey = `${basePath}/${now}.csv`;

    // 3. Sposta il file dalla path temporanea Athena alla path finale
    await moveAthenaResult(bucket, athenaResultPath, targetKey);
    await convertCsvSeparator(bucket, targetKey);

    // 4. Genera presigned URL per il download e restituiscilo
    const downloadUrl = await generatePresignedDownloadUrl(bucket, targetKey);
    return {
        downloadUrl,
        expiresIn: 300,
        key: targetKey,
    };
};
