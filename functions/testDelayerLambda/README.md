# TestDelayerLambda

Lambda (Node 20) utile per eseguire test automatici sull'algoritmo di pianificazione.

La lambda utilizza un dispatcher per supportare più tipi di operazioni utili per il testing.

## Funzionalità principali

| Operation | Descrizione |
|-----------|-------------|
| **IMPORT_DATA** | Scarica un CSV da S3, lo parse-a (separator `;`) e inserisce le righe nella tabella DynamoDB `pn-DelayerPaperDelivery` con `BatchWrite` (retry su `UnprocessedItems`). |

> Aggiungi nuove operazioni creando un nuovo modulo e registrandolo in `eventHandler.js` dentro l’oggetto `OPERATIONS`.

## Struttura del progetto

```
.
├── index.js               # Entrypoint Lambda
├── package.json           # Dipendenze e script
├── jest.config.js         # Config Jest
├── src/
│   └── app/
│       ├── eventHandler.js  # Dispatcher delle operazioni
│       ├── importData.js  # Implementazione operazione IMPORT_DATA
│   └── test/
│       ├── lambda.test.js # Test unitari (Jest + aws-sdk-client-mock)
│       └── sample.csv     # Fixture di esempio
└── README.md
```


## Requisiti

* **Node.js 20.x** (runtime Lambda)
* **npm 9.x**

## Variabili d’ambiente

| Nome           | Descrizione                                 | Obbligatoria |
|----------------|---------------------------------------------|--------------|
| `BUCKET_NAME`  | Bucket S3 contenente il CSV                 | ✅ |
| `OBJECT_KEY`   | Key/Path dell’oggetto CSV                   | ✅ |

## Setup locale
Dalla cartella testDelayerLambda

```bash
# Installa dipendenze
npm install

# Esegui test unitari
npm test
