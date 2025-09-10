const fs = require("fs");
const path = require("path");
const { parseArgs } = require("util");
const csvGenerator = require('./csvGenerator');
const { importData } = require("./importData");
const { SafeStorageClient } = require('./SafeStorageClient');

const options = {
    options: {
        env: { type: 'string', short: 'e', default: 'local' }
    }
};
const parsedArgs = parseArgs(options);
const env = parsedArgs.values.env;

const folderPath = path.join(__dirname, "spedizioni");
const paperDeliveryTableName = "pn-DelayerPaperDelivery";
const countersTableName = "pn-PaperDeliveryCounters";
const SAFE_STORAGE_URL = env === 'test' ? 'http://localhost:8889' : 'http://localhost:8888';
const MODULI_COMMESSA_FOLDER = './moduliCommessa';

async function processCsvFiles() {
    console.log("=== PROCESSING CSV FILES ===");
    const files = fs.readdirSync(folderPath).filter(file => file.endsWith(".csv"));
    for (const file of files) {
        const filePath = path.join(folderPath, file);
        const csvContent = fs.readFileSync(filePath, "utf8");
        try {
            const result = await importData([paperDeliveryTableName, countersTableName], csvContent, env);
            console.log(`File ${file} importato:`, result);
        } catch (err) {
            console.error(`Errore importando ${file}:`, err);
        }
    }
}

async function processJsonFiles() {
    try {
        console.log("=== PROCESSING JSON FILES ===");
        const safeStorageClient = new SafeStorageClient(SAFE_STORAGE_URL);

        if (!fs.existsSync(MODULI_COMMESSA_FOLDER)) {
            console.error(`La cartella ${MODULI_COMMESSA_FOLDER} non esiste!`);
            console.log(`Creala e inserisci i file JSON da processare.`);
            return;
        }

        const files = fs.readdirSync(MODULI_COMMESSA_FOLDER);
        const jsonFiles = files.filter(file =>
            path.extname(file).toLowerCase() === '.json'
        );

        if (jsonFiles.length === 0) {
            console.log('Nessun file JSON trovato nella cartella moduliCommessa');
            return;
        }

        console.log(`Trovati ${jsonFiles.length} file JSON da processare:`);
        jsonFiles.forEach((file, index) =>
            console.log(`  ${index + 1}. ${file}`)
        );

        const results = [];
        const startTime = Date.now();

        for (let i = 0; i < jsonFiles.length; i++) {
            const fileName = jsonFiles[i];
            const filePath = path.join(MODULI_COMMESSA_FOLDER, fileName);

            console.log(`\n[${i + 1}/${jsonFiles.length}] Processando: ${fileName}`);

            const result = await safeStorageClient.processJsonFile(filePath);
            results.push(result);

            if (result.success) {
                console.log(`Completato con successo`);
            } else {
                console.log(`Errore: ${result.error}`);
            }

            if (i < jsonFiles.length - 1) {
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        }

        const endTime = Date.now();
        const duration = Math.round((endTime - startTime) / 1000);

        console.log('\n=== RIEPILOGO JSON PROCESSING ===');
        const successful = results.filter(r => r.success);
        const failed = results.filter(r => !r.success);

        console.log(`Tempo totale: ${duration} secondi`);
        console.log(`File processati: ${results.length}`);
        console.log(`Successi: ${successful.length}`);
        console.log(`Errori: ${failed.length}`);

        if (successful.length > 0) {
            console.log('\nFile caricati con successo:');
            successful.forEach((result, index) => {
                console.log(`  ${index + 1}. ${path.basename(result.filePath)}`);
                console.log(`     Key: ${result.key}`);
            });
        }

        if (failed.length > 0) {
            console.log('\nFile con errori:');
            failed.forEach((result, index) => {
                console.log(`  ${index + 1}. ${path.basename(result.filePath)}`);
                console.log(`     Errore: ${result.error}`);
            });
        }

        const successRate = Math.round((successful.length / results.length) * 100);
        console.log(`\nProcessing completato! Tasso di successo: ${successRate}%`);

    } catch (error) {
        console.error('Errore critico durante il processing JSON:', error.message);
        throw error;
    }
}

async function main() {
    try {
        console.log("=== GENERAZIONE FILE CSV E JSON ===");
         //await csvGenerator.main();

        console.log("\n=== UPLOAD JSON SU SAFESTORAGE ===");
        await processJsonFiles();

        console.log("\n=== IMPORT CSV SU DYNAMODB ===");
        //processCsvFiles();

        console.log('\nTutte le operazioni completate!');
    } catch (error) {
        console.error('Errore durante l\'esecuzione:', error.message);
        process.exit(1);
    }
}

main();