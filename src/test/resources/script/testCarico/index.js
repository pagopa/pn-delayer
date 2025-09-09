const fs = require("fs");
const path = require("path");
const { importData } = require("./importData");
const { SafeStorageClient } = require('./SafeStorageClient');

// Configurazione esistente per CSV
const folderPath = path.join(__dirname, "spedizioni");
const paperDeliveryTableName = "pn-DelayerPaperDelivery";
const countersTableName = "pn-PaperDeliveryCounters";

// Nuova configurazione per JSON
const SAFE_STORAGE_URL = 'http://localhost:8888';
const MODULI_COMMESSA_FOLDER = './moduliCommessa';

// Funzione esistente per processare CSV
function processCsvFiles() {
    console.log("=== PROCESSING CSV FILES ===");

    fs.readdirSync(folderPath)
        .filter(file => file.endsWith(".csv"))
        .forEach(async (file) => {
            const filePath = path.join(folderPath, file);
            const csvContent = fs.readFileSync(filePath, "utf8");
            try {
                const result = await importData([paperDeliveryTableName, countersTableName], csvContent);
                console.log(`File ${file} importato:`, result);
            } catch (err) {
                console.error(`Errore importando ${file}:`, err);
            }
        });
}

// Nuova funzione per processare JSON
async function processJsonFiles() {
    try {
        console.log("=== PROCESSING JSON FILES ===");

        // Inizializza il client SafeStorage
        const safeStorageClient = new SafeStorageClient(SAFE_STORAGE_URL);

        // Verifica che la cartella esista
        if (!fs.existsSync(MODULI_COMMESSA_FOLDER)) {
            console.error(`La cartella ${MODULI_COMMESSA_FOLDER} non esiste!`);
            console.log(`Creala e inserisci i file JSON da processare.`);
            return;
        }

        // Leggi tutti i file nella cartella
        const files = fs.readdirSync(MODULI_COMMESSA_FOLDER);

        // Filtra solo i file JSON
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

        // Processa ogni file JSON sequenzialmente
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

            // Pausa breve tra i file per evitare di sovraccaricare il server
            if (i < jsonFiles.length - 1) {
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        }

        // Stampa il riepilogo finale
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

// Funzione principale che esegue entrambi i processing
async function main() {
    try {
        // Processa i CSV
      //  processCsvFiles();

        // wait
        //await new Promise(resolve => setTimeout(resolve, 1000));

        // Processa i JSON
        await processJsonFiles();

        console.log('\nTutte le operazioni completate!');

    } catch (error) {
        console.error('Errore durante l\'esecuzione:', error.message);
        process.exit(1);
    }
}

main();