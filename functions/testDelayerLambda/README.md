# TestDelayerLambda

Lambda (Node 20) utile per eseguire test automatici sull'algoritmo di pianificazione.

La lambda utilizza un dispatcher per supportare più tipi di operazioni utili per il testing.

## Operazioni disponibili

| Nome                           | Descrizione                                                                                                                                                         | Parametri (`event.parameters`)                                                                                                                                                                                                   |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **IMPORT_DATA**                | Importa un CSV da S3 nella tabella `pn-DelayerPaperDelivery` tramite scritture `BatchWrite`.                                                                        | `["delayerPaperDeliveryTableName", "paperDeliveryCountersTableName","filename", "deliveryWeek"]` deliveryWeek opzionale                                                                                                          |
| **DELETE_DATA**                | Cancella i dati generati dal test dalle tabelle dynamo interessate partendo da un CSV presebte su S3 tramite cancellazioni `BatchWrite`.                            | `["delayerPaperDeliveryTableName","deliveryDriverUsedCapacityTableName", "usedSenderLimitTableName", "paperDeliveryCountersTableName","filename", "]` filename opzionale                                                         |
| **GET_USED_CAPACITY**          | Legge la capacità utilizzata per la combinazione `unifiedDeliveryDriver~geoKey` alla `deliveryDate` indicata, dalla tabella `pn-PaperDeliveryDriverUsedCapacities`. | `[ "paperDeliveryDriverUsedCapacitiesTableName", unifiedDeliveryDriver", "geoKey", "deliveryDate (ISO‑8601 UTC)" ]`                                                                                                              |
| **GET_BY_REQUEST_ID**          | Restituisce **tutte** le righe aventi lo stesso `requestId` interrogando la GSI **`requestId-CreatedAt-index`** della tabella `pn-DelayerPaperDelivery`.            | `[ requestId ]`                                                                                                                                                                                                                  |
| **RUN_ALGORITHM**              | Avvia la Step Function BatchWorkflowStateMachine passandole i parametri statici per i nomi delle tabelle.                                                           | `["delayerPaperDeliveryTableName","deliveryDriverCapacityTabelName","deliveryDriverUsedCapacityTableName", "senderLimitTableName","usedSenderLimitTableName", "paperDeliveryCountersTableName","printCapacity", "deliveryWeek"]` |
| **DELAYER_TO_PAPER_CHANNEL**   | Avvia la Step Function DelayerToPaperChannelStateMachine passandole i parametri statici per i nomi delle tabelle.                                                   | `["delayerPaperDeliveryTableName","paperDeliveryCountersTableName"]`                                                                                                                                                             |
| **GET_STATUS_EXECUTION**       | Restituisce lo stato di una specifica esecuzione di una Step Function                                                                                               | `["executionArn"]`                                                                                                                                                                                                               |
| **GET_PAPER_DELIVERY**         | Restituisce le spedizioni data `deliveryDate` e `workFlowStep`.                                                                                                     | `["delayerPaperDeliveryTableName", "deliveryDate", "workFlowStep", "lastEvaluatedKey"]`  lastEvaluatedKey opzionale                                                                                                              |
| **GET_SENDER_LIMIT**           | Restituisce le stime dichiarate dai mittenti filtrate per settimana di spedizione e provincia dalla tabella `pn-PaperDeliverySenderLimit`.                          | `[ "deliveryDate (yyyy-MM-dd)", "province", "lastEvaluatedKey" ]` lastEvaluatedKey opzionale                                                                                                                                     |
| **GET_PRESIGNED_URL**          | Restituisce l'url su cui fare l'upload dei csv delle spedizioni o delle capacità dichiarate dai recapitisti                                                         | `["filename","checksum"]`                                                                                                                                                                                                        |
| **GET_DECLARED_CAPACITY**      | Legge la capacità dichiarata di un driver per una specifica data ed area geografica.                                                                                | `["deliveryDriverCapacityTabelName", province", "deliveryDate"]`                                                                                                                                                                 | 
| **INSERT_MOCK_CAPACITIES**     | Importa un CSV da S3 nella tabella `pn-PaperDeliveryDriverCapacitiesMock`.                                                                                          | `["deliveryDriverCapacityTableName","filename"]`                                                                                                                                                                                 |
| **GET_PRINT_CAPACITY_COUNTER** | Restituisce l'entità del contatore per la verifica della capacità di stampa settimanale.                                                                            | `["paperDeliveryCountersTableName, deliveryDate"]`                                                                                                                                                                               |

### Esempi di payload

*IMPORT_DATA*

```json
{
  "operationType": "IMPORT_DATA",
  "parameters": ["pn-DelayerPaperDelivery", "pn-PaperDeliveryCounters","example.csv", "2025-10-03"]
}
```

#### CAMPI DEL CSV
| Nome                   | Descrizione                                                                                                 |
|------------------------|-------------------------------------------------------------------------------------------------------------|
| **requestId**          | Identificativo spedizione nel formato PREPARE_ANALOG_DOMICILE.IUN_<iun>.RECINDEX_<index>.ATTEMPT_<attempt>. | 
| **notificationSentAt** | Data deposito notifica in formato ISO 8601 con fuso orario UTC. Esempio 2025-01-01T00:00:00Z.               |
| **prepareRequestDate** | Data deposito notifica in formato ISO 8601 con fuso orario UTC. Esempio 2025-01-01T00:00:00Z.               |
| **productType**        | Prodotto postale (AR, 890, RS).                                                                             |
| **senderPaId**         | Identificativo mittente della notifica.                                                                     |
| **province**           | Sigla ufficiale a due cifre della provincia della spedizione. Esempio: NA.                                  |
| **cap**                | Cap della spedizione.                                                                                       |
| **attempt**            | Intero che indica il numero di tentativo della spedizione (0=primo, 1=secondo).                             |
| **iun**                | Identificativo della notifica.                                                                              |


*DELETE_DATA*

```json
{
  "operationType": "DELETE_DATA",
  "parameters": ["pn-DelayerPaperDelivery","pn-PaperDeliveryDriverUsedCapacities",
    "pn-PaperDeliveryUsedSenderLimit", "pn-PaperDeliveryCounters","example.csv"]
}
```

#### CAMPI DEL CSV
| Nome                   | Descrizione                                                                                                 |
|------------------------|-------------------------------------------------------------------------------------------------------------|
| **requestId**          | Identificativo spedizione nel formato PREPARE_ANALOG_DOMICILE.IUN_<iun>.RECINDEX_<index>.ATTEMPT_<attempt>. | 
| **notificationSentAt** | Data deposito notifica in formato ISO 8601 con fuso orario UTC. Esempio 2025-01-01T00:00:00Z.               |
| **prepareRequestDate** | Data deposito notifica in formato ISO 8601 con fuso orario UTC. Esempio 2025-01-01T00:00:00Z.               |
| **productType**        | Prodotto postale (AR, 890, RS).                                                                             |
| **senderPaId**         | Identificativo mittente della notifica.                                                                     |
| **province**           | Sigla ufficiale a due cifre della provincia della spedizione. Esempio: NA.                                  |
| **cap**                | Cap della spedizione.                                                                                       |
| **attempt**            | Intero che indica il numero di tentativo della spedizione (0=primo, 1=secondo).                             |
| **iun**                | Identificativo della notifica.                                                                              |



*GET_USED_CAPACITY*

```json
{
  "operationType": "GET_USED_CAPACITY",
  "parameters": ["pn-PaperDeliveryDriverUsedCapacities", "Sailpost", "87100", "2025-06-30T00:00:00Z"]
}
```

*GET_SENDER_LIMIT*

- Senza lastEvaluatedKey

```json
{
  "operationType": "GET_SENDER_LIMIT",
  "parameters": ["2025-06-30", "RM"]
}
```
- Con lastEvaluatedKey
```json
{
  "operationType": "GET_SENDER_LIMIT",
  "parameters": ["2025-06-30", "RM", "<lek>"]
}
```
*GET_BY_REQUEST_ID*
```json
{
  "operationType": "GET_BY_REQUEST_ID",
  "parameters": ["PREPARE_ANALOG_DOMICILE.IUN_ADTA-XNPA-UXVL-202506-M-1.RECINDEX_0.ATTEMPT_0"]
}
```

*GET_STATUS_EXECUTION*
```json
{
  "operationType": "GET_STATUS_EXECUTION",
  "parameters": ["executionArn"]
}
```

*GET_PAPER_DELIVERY*
```json
{
  "operationType": "GET_PAPER_DELIVERY",
  "parameters": ["pn-DelayerPaperDelivery", "2025-07-07", "EVALUATE_SENDER_LIMIT"]
}
```

*GET_DECLARED_CAPACITY*
```json
{
  "operationType": "GET_DECLARED_CAPACITY",
  "parameters": ["pn-PaperDeliveryDriverCapacities","province", "deliveryDate"]
}
```

*RUN_ALGORITHM*
```json
{
  "operationType": "RUN_ALGORITHM",
  "parameters": ["pn-DelayerPaperDelivery","pn-PaperDeliveryDriverCapacities", "pn-PaperDeliveryDriverUsedCapacities", 
    "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit", "pn-PaperDeliveryCounters","180000", "2025-10-06"]
}
```

#### Parametri in input di RUN_ALGORITHM
| Nome                                      | Descrizione                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Tabella valorizzata da                                          | 
|-------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------| 
| **paperDeliveryTableName**                | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-DelayerPaperDelivery) contenente il workflow delle richieste di spedizioni mock.                                                                                                                                                                                                                                                                                                                                                                    | operaz. IMPORT_DATA                                             |
| **deliveryDriverCapacitiesTableName**     | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1624571910/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+1#Tabella-pn-PaperDeliveryDriverCapacities) su cui leggere le capacità dei recapitisti.                                                                                                                                                                                                                                                                                                                                                                          | import CSV delle capacità                                       | 
| **deliveryDriverUsedCapacitiesTableName** | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-PaperDeliveryDriverUsedCapacities) su cui vengono contate le spedizioni per ogni coppia recapitista-provincia e recapitista-CAP.                                                                                                                                                                                                                                                                                                                    | alg. di pianificazione                                          |
| **senderLimitTableName**                  | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-PaperDeliverySenderLimit) su cui leggere le stime dei mittenti.                                                                                                                                                                                                                                                                                                                                                                                     | TODO                                                            |
| **senderUsedLimitTableName**              | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-PaperDeliveriesSenderUsedLimit) su cui l’algoritmo tiene il conteggio delle spedizioni che hanno superato il check del limite del mittente per la tripletta PaId~ProductType~Province.                                                                                                                                                                                                                                                              | alg. di pianificazione                                          |
| **countersTableName**                     | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-PaperDeliveriesCounter) su cui l’algoritmo: 1. salve le coppie Province~UnifiedDeliveryDriver (prefisso chiave EXCEED~) per le quali esistono delle eccedenze. 2. legge il numero di spedizioni che devono essere escluse dal check dei limiti del mittente per la coppa ProductType~Province (prefisso chiave EXCLUDE~). L’algoritmo si aspetta che la tabella sia già  - eventualmente - valorizzata per il caso 2 (righe con prefisso EXCLUDE~.. | operaz. IMPORT_DATA per quanto riguarda RS e secondo tentativi. |
| **printCapacity**                         | Intero che indica il numero di capacità di stampa giornaliera.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | ---                                                             |
| **deliveryWeek**                          | Settimana di spedizione nel formato yyyy-MM-dd                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | ---                                                             |


*DELAYER_TO_PAPER_CHANNEL*
```json
{
  "operationType": "DELAYER_TO_PAPER_CHANNEL",
  "parameters": ["pn-DelayerPaperDelivery","pn-PaperDeliveryCounters"]
}
```
#### Parametri in input di DELAYER_TO_PAPER_CHANNEL
| Nome                                      | Descrizione                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Tabella valorizzata da                                          | 
|-------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------| 
| **paperDeliveryTableName**                | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-DelayerPaperDelivery) contenente il workflow delle richieste di spedizioni mock.                                                                                                                                                                                                                                                                                                                                                                    | operaz. IMPORT_DATA                                             |
| **countersTableName**                     | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-PaperDeliveriesCounter) su cui l’algoritmo: 1. salve le coppie Province~UnifiedDeliveryDriver (prefisso chiave EXCEED~) per le quali esistono delle eccedenze. 2. legge il numero di spedizioni che devono essere escluse dal check dei limiti del mittente per la coppa ProductType~Province (prefisso chiave EXCLUDE~). L’algoritmo si aspetta che la tabella sia già  - eventualmente - valorizzata per il caso 2 (righe con prefisso EXCLUDE~.. | operaz. IMPORT_DATA per quanto riguarda RS e secondo tentativi. |
| **deliveryDateDayOfWeek**                 | Intero che indica il giorno della settimana su cui l'algoritmo di pianificazione deve partire (default = 1, cioè lunedì).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | ---                                                             |

*GET_PRESIGNED_URL*
```json
{
  "operationType": "GET_PRESIGNED_URL",
  "parameters": ["example.csv","abcd1234efgh5678ijkl9012mnop3456"]
}
```

*GET_PRINT_CAPACITY_COUNTER*
```json
{
  "operationType": "GET_PRINT_CAPACITY_COUNTER",
  "parameters": ["pn-PaperDeliveryCounters","2025-11-24"]
}
```

### Output GET_DECLARED_CAPACITY

* Items trovati → 
  ```json
  {
    "items":[{
      "unifiedDeliveryDriverGeokey": "Sailpost~87100",
      "deliveryDate": "2025-06-30T00:00:00Z",
      "geoKey": "87100",
      "unifiedDeliveryDriver": "Sailpost",
      "usedCapacity": 572,
      "capacity": 1000
    }]
  }
  ```
* Item not trovati → `{ "items": [] }`
* 
*INSERT_MOCK_CAPACITIES*
```json
{
  "operationType": "INSERT_MOCK_CAPACITIES",
  "parameters": ["pn-PaperDeliveryDriverCapacitiesMock","example.csv"]
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

### Output GET_SENDER_LIMIT

* Items trovati → array di oggetti, ad esempio:
  ```json
  {
    "items":[
      {
        "pk": "abc14d59-1e1f-4ghi-lf3m-n46161o0pq95~AR~RM",
        "deliveryDate": "2025-09-29",
        "weeklyEstimate": 100,
        "monthlyEstimate": 400,
        "originalEstimate": 500,
        "paId": "abc14d59-1e1f-4ghi-lf3m-n46161o0pq95",
        "productType": "AR",
        "province": "RM"
      }
    ],
    "lastEvaluatedKey": {}
  }
  ```

* Item assente → 
```json
 { "items": [] }
```

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

### Output GET_PAPER_DELIVERY

* Items presenti →
  ```json
  {
    "items":[{
      "iun": "AUTJ-PUKM-KDAJ-250017-T-1",
      "notificationSentAt": "2025-07-01T00:17:00Z",
      "workflowStep": "EVALUATE_PRINT_CAPACITY",
      "priority": 1,
      "tenderId": "20250319",
      "attempt": 0,
      "createdAt": "2025-09-10T17:05:48.240305919Z",
      "senderPaId": "rankingRS_2nd",
      "cap": "CAP5",
      "province": "P2",
      "requestId": "tcRanking_RS_2nd_1",
      "sk": "1~2025-07-01T00:17:05Z~tcRanking_RS_2nd_1",
      "pk": "2025-09-08~EVALUATE_PRINT_CAPACITY",
      "prepareRequestDate": "2025-07-01T00:17:04Z",
      "productType": "RS",
      "unifiedDeliveryDriver": "driverRankingRS_2nd"
    }],
  "lastEvaluatedKey":{}
  }
  ```
* Items assente →
  ```json
  { "items": [] }
  ```

### Output GET_PRESIGNED_URL

  ```json
  {
    "uploadUrl": "",
    "key" :  "<filename>",
    "requiredHeaders": {
      "Content-Type": "text/csv",
      "x-amz-checksum-sha256": "abcd1234efgh5678ijkl9012mnop3456"
    },
   "expiresIn": 300
  }
  ```

### Output GET_STATUS_EXECUTION
Viene restituito un oggetto con i dettagli dell’esecuzione della Step Function.

Un esempio di risposta è il seguente:
```json
{
  "executionArn": "arn:aws:states:<REGIONE>:<ACCOUNT_ID>:execution:<NOME_STATE_MACHINE>:<NOME_ESECUZIONE>",
  "status": "FAILED",
  "startDate": "2025-09-24T08:27:46.279Z",
  "stopDate": "2025-09-24T08:32:10.123Z",
  "error": "Process exited with error code 1",
  "cause": "Unexpected input format"
}
```

### Output GET_PRINT_CAPACITY_COUNTER

* Items presenti →
```json
{
  "pk": "PRINT",
  "sk": "2025-11-24",
  "dailyExecutionCounter": 4,
  "dailyExecutionNumber": 4,
  "dailyPrintCapacity": 20,
  "lastEvaluatedKeyNextWeek": {},
  "lastEvaluatedKeyPhase2": {
  "pk": "2025-11-24~EVALUATE_PRINT_CAPACITY",
  "sk": "1~2025-07-02T00:48:00Z~tcRanking_RS_2"
  },
  "numberOfShipments": 70,
  "sentToNextWeek": 0,
  "sentToPhaseTwo": 11,
  "ttl": 1766571878068,
  "weeklyPrintCapacity": 140
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
│       ├── getSenderLimit.js                           # Implementazione operazione GET_SENDER_LIMIT
│       ├── importData.js                               # Implementazione operazione IMPORT_DATA
│       ├── runAlgorithm.js                             # Implementazione operazione RUN_ALGORITHM
│       ├── getDeclaredCapacity.js                      # Implementazione operazione GET_DECLARED_CAPACITY
│       ├── getPaperDelivery.js                         # Implementazione operazione GET_PAPER_DELIVERY
│       ├── getPresignedUrl.js                          # Implementazione operazione GET_PRESIGNED_URL
│       ├── getStatusExecution.js                       # Implementazione operazione GET_STATUS_EXECUTION
│       ├── insertMockCapacities.js                     # Implementazione operazione INSERT_MOCK_CAPACITIES
│       └── getPrintCapacityCounter.js                  # Implementazione operazione GET_PRINT_CAPACITY_COUNTER
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
 