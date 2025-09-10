# Test Carico - Script di Generazione e Importazione Dati

Questa cartella contiene script Node.js per la generazione e l'importazione massiva di dati di test (spedizioni e moduli commessa) su DynamoDB e Safe Storage, utili per test di carico.

## Struttura dei file

- `csvGenerator.js`: genera file CSV di spedizioni e file JSON di moduli commessa per diverse PA.
- `importData.js`: importa i dati dai file CSV su DynamoDB, gestendo anche i contatori per le spedizioni.
- `SafeStorageClient.js`: gestisce l'upload dei file JSON su Safe Storage tramite API REST (POST metadati, PUT file).
- `index.js`: script principale che coordina la generazione, l'importazione e l'upload dei file.

## Prerequisiti

- Node.js >= 18
- npm 9.x
- Accesso a DynamoDB (localstack o AWS) e Safe Storage (endpoint REST)
- Installare le dipendenze con:
     ```
     npm install
     ```
  
## Utilizzo

1. **Generazione file di test**
   - Eseguire:
     ```
     node src/test/resources/script/testCarico/index.js
     ```
   - Verranno generati:
     - 100 file CSV da 30.000 spedizioni ciascuno (`spedizioni/`)
     - 300 file JSON di moduli commessa (`moduliCommessa/`)

2. **Upload file JSON su Safe Storage**
   - Lo script carica i file JSON generati tramite API REST su Safe Storage.
   - I risultati (successi/errori) vengono riepilogati a fine processo.

3. **Importazione CSV su DynamoDB**
   - Lo script può importare i file CSV su DynamoDB, scrivendo le spedizioni e aggiornando i contatori.
   - Per abilitare la generazione/importazione, decommentare le chiamate corrispondenti in `index.js`.

## Parametri

- È possibile specificare l'ambiente (`local`, `dev`, `test`) tramite parametro:
    ```
     node index.js --env <env>
    ```
  Se non specificato, lo script verrà eseguito in modalità locale.

## Note

- I file generati sono pensati per simulare carichi elevati e coprono tutte le regioni italiane.
- I moduli commessa JSON sono generati solo per le PA dalla 101 alla 400.
- Verificare che le cartelle `spedizioni/` e `moduliCommessa/` esistano prima dell'esecuzione.
