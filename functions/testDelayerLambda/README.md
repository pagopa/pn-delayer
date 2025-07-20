# TestDelayerLambda

Lambda (Node 20) utile per eseguire test automatici sull'algoritmo di pianificazione.

La lambda utilizza un dispatcher per supportare più tipi di operazioni utili per il testing.

## Operazioni disponibili

| Nome | Descrizione | Parametri (`event.parameters`) |
|------|-------------|--------------------------------|
| **IMPORT_DATA** | Importa un CSV da S3 nella tabella `pn-DelayerPaperDelivery` tramite scritture `BatchWrite`. | _Nessuno_ → passare un array vuoto `[]` |
| **GET_USED_CAPACITY** | Legge la capacità utilizzata per la combinazione `unifiedDeliveryDriver~geoKey` alla `deliveryDate` indicata, dalla tabella `pn-PaperDeliveryDriverUsedCapacities`. | `[ "unifiedDeliveryDriver", "geoKey", "deliveryDate (ISO‑8601 UTC)" ]` |
| **GET_BY_REQUEST_ID**  | Restituisce **tutte** le righe aventi lo stesso `requestId` interrogando la GSI **`requestId-CreatedAt-index`** della tabella `pn-DelayerPaperDelivery`. | `[ requestId ]`                                                              |
| **RUN_ALGORITHM** | Avvia la Step Function BatchWorkflowStateMachine passandole i parametri statici per i nomi delle tabelle. | [] |

### Esempi di payload

*IMPORT_DATA*

```json
{
  "operationType": "IMPORT_DATA",
  "parameters": []
}
```

*GET_USED_CAPACITY*

```json
{
  "operationType": "GET_USED_CAPACITY",
  "parameters": ["Sailpost", "87100", "2025-06-30T00:00:00Z"]
}
```

*GET_BY_REQUEST_ID*
```json
{
  "operationType": "GET_BY_REQUEST_ID",
  "parameters": ["PREPARE_ANALOG_DOMICILE.IUN_ADTA-XNPA-UXVL-202506-M-1.RECINDEX_0.ATTEMPT_0"]
}
```

*RUN_ALGORITHM*
```json
{
  "operationType": "RUN_ALGORITHM",
  "parameters": []
}
```

### Output GET_USED_CAPACITY

* Item trovato → oggetto completo, ad esempio:
  ```json
  {
    "unifiedDeliveryDriverGeokey": "Sailpost~87100",
    "deliveryDate": "2025-06-30T00:00:00Z",
    "geoKey": "87100",
    "unifiedDeliveryDriver": "Sailpost",
    "usedCapacity": 572,
    "capacity": 1000
  }
  ```
* Item assente → `{ "message": "Item not found" }`

### Output GET_BY_REQUEST_ID
Se trovate, viene restituito un array di oggetti (tutte le righe con quel requestId); se non ci sono risultati l’array è vuoto ([]).

Un esempio di risposta è il seguente:
```json
[
  {
    "pk": "2025-07-07~EVALUATE_SENDER_LIMIT",
    "sk": "NA~2025-07-07~PREPARE_ANALOG_DOMICILE.IUN_ADTA-XNPA-UXVL-202506-M-1.RECINDEX_0.ATTEMPT_0",
    "attempt": "0",
    "cap": "80124",
    "createdAt": "2025-01-01T00:00:00Z",
    "deliveryDate": "1970-01-05T00:00:00Z",
    "iun": "ADTA-XNPA-UXVL-202506-M-1",
    "notificationSentAt": "2025-01-01T00:00:00Z",
    "prepareRequestDate": "2025-01-01T00:00:00Z",
    "priority": "",
    "productType": "AR",
    "province": "NA",
    "recipientId": "",
    "requestId": "PREPARE_ANALOG_DOMICILE.IUN_ADTA-XNPA-UXVL-202506-M-1.RECINDEX_0.ATTEMPT_0",
    "senderPaId": "idMittente1",
    "tenderId": "",
    "unifiedDeliveryDriver": ""
  }
]
```
### Output GET_BY_REQUEST_ID
```json
{
  "PaperDeliveryTableName":"pn-DelayerPaperDelivery",
  "DeliveryDriverCapacityTableName":"pn-PaperDeliveryDriverCapacities",
  "DeliveryDriverUsedCapacityTableName":"pn-PaperDeliveryDriverUsedCapacities",
  "EstimateSendersTableName":"pn-PaperDeliveryEstimateSenders",
  "SenderUsedLimitTableName": "pn-PaperDeliveriesSenderUsedLimit",
  "PrintCapacityCounterTableName": "pn-PaperDeliveriesPrintCapacityCounter",
  "CounterTableName": "pn-PaperDeliveriesCounter"
}
```


> Aggiungi nuove operazioni creando un nuovo modulo e registrandolo in `eventHandler.js` dentro l’oggetto `OPERATIONS`.

## Struttura del progetto

```
.
├── index.js               # Entrypoint Lambda
├── package.json           # Dipendenze e script
├── src/
│   └── app/
│       ├── eventHandler.js                             # Dispatcher delle operazioni
│       ├── getDelayerPaperDeliveriesByRequestId.js.js  # Implementazione operazione GET_BY_REQUEST_ID
│       ├── getUsedCapacity.js                          # Implementazione operazione GET_USED_CAPACITY
│       ├── importData.js                               # Implementazione operazione IMPORT_DATA
│       ├── runAlgorithm.js                             # Implementazione operazione RUN_ALGORITHM
│   └── test/
│       ├── eventHandler.test.js # Test unitari (Nyc + aws-sdk-client-mock)
│       └── sample.csv     # Fixture di esempio
└── README.md
```


## Requisiti

* **Node.js 20.x** (runtime Lambda)
* **npm 9.x**

## Variabili d’ambiente

| Nome          | Descrizione                 | Obbligatoria |
|---------------|-----------------------------|--------------|
| `BUCKET_NAME` | Bucket S3 contenente il CSV | ✅ |
| `OBJECT_KEY`  | Key/Path dell’oggetto CSV   | ✅ |
| `SFN_ARN`     | ARN della Step Function     | ✅ |

## Setup locale
Dalla cartella testDelayerLambda

```bash
# Installa dipendenze
npm install

# Esegui test unitari
npm test
