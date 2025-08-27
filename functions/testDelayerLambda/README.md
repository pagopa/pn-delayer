# TestDelayerLambda

Lambda (Node 20) utile per eseguire test automatici sull'algoritmo di pianificazione.

La lambda utilizza un dispatcher per supportare più tipi di operazioni utili per il testing.

## Operazioni disponibili

| Nome                         | Descrizione                                                                                                                                                         | Parametri (`event.parameters`)                                         |
|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| **IMPORT_DATA**              | Importa un CSV da S3 nella tabella `pn-DelayerPaperDelivery` tramite scritture `BatchWrite`.                                                                        | `["fileName"]` opzionale                                               |
| **DELETE_DATA**              | Cancella i dati generati dal test dalle tabelle dynamo interessate partendo da un CSV presebte su S3 tramite cancellazioni `BatchWrite`.                            | `["fileName"]` opzionale                                               |
| **GET_USED_CAPACITY**        | Legge la capacità utilizzata per la combinazione `unifiedDeliveryDriver~geoKey` alla `deliveryDate` indicata, dalla tabella `pn-PaperDeliveryDriverUsedCapacities`. | `[ "unifiedDeliveryDriver", "geoKey", "deliveryDate (ISO‑8601 UTC)" ]` |
| **GET_BY_REQUEST_ID**        | Restituisce **tutte** le righe aventi lo stesso `requestId` interrogando la GSI **`requestId-CreatedAt-index`** della tabella `pn-DelayerPaperDelivery`.            | `[ requestId ]`                                                        |
| **RUN_ALGORITHM**            | Avvia la Step Function BatchWorkflowStateMachine passandole i parametri statici per i nomi delle tabelle.                                                           | `["printCapacity", "deliveryDateDayOfWeek"]` entrambi opzionali        |
| **DELAYER_TO_PAPER_CHANNEL** | Avvia la Step Function DelayerToPaperChannelStateMachine passandole i parametri statici per i nomi delle tabelle.                                                   | `["deliveryDateDayOfWeek"]` opzionale                                  |

### Esempi di payload

*IMPORT_DATA*

```json
{
  "operationType": "IMPORT_DATA",
  "parameters": ["example.csv"]
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
  "parameters": ["example.csv"]
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
  "parameters": ["pn-DelayerPaperDelivery","pn-PaperDeliveryDriverCapacities","pn-PaperDeliveryDriverUsedCapacities",
    "pn-PaperDeliverySenderLimit","pn-PaperDeliveryUsedSenderLimit","pn-PaperDeliveryPrintCapacity",
    "pn-PaperDeliveryCounters","180000","1"]
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
| **printCapacityTableName**                | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-PaperDeliveriesPrintCapacity) su cui l’algoritmo tiene il conteggio delle spedizioni stampate giornalmente e settimanalmente.                                                                                                                                                                                                                                                                                                                       | alg. di pianificazione                                          |
| **countersTableName**                     | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-PaperDeliveriesCounter) su cui l’algoritmo: 1. salve le coppie Province~UnifiedDeliveryDriver (prefisso chiave EXCEED~) per le quali esistono delle eccedenze. 2. legge il numero di spedizioni che devono essere escluse dal check dei limiti del mittente per la coppa ProductType~Province (prefisso chiave EXCLUDE~). L’algoritmo si aspetta che la tabella sia già  - eventualmente - valorizzata per il caso 2 (righe con prefisso EXCLUDE~.. | operaz. IMPORT_DATA per quanto riguarda RS e secondo tentativi. |
| **printCapacity**                         | Intero che indica il numero di capacità di stampa giornaliera.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | ---                                                             |
| **deliveryDateDayOfWeek**                 | Intero che indica il giorno della settimana su cui l'algoritmo di pianificazione deve partire (default = 1, cioè lunedì).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | ---                                                             |


*DELAYER_TO_PAPER_CHANNEL*
```json
{
  "operationType": "DELAYER_TO_PAPER_CHANNEL",
  "parameters": ["pn-DelayerPaperDelivery","pn-PaperDeliveryCounters","1"]
}
```
#### Parametri in input di DELAYER_TO_PAPER_CHANNEL
| Nome                                      | Descrizione                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Tabella valorizzata da                                          | 
|-------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------| 
| **paperDeliveryTableName**                | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-DelayerPaperDelivery) contenente il workflow delle richieste di spedizioni mock.                                                                                                                                                                                                                                                                                                                                                                    | operaz. IMPORT_DATA                                             |
| **countersTableName**                     | Indica il nome della [tabella](https://pagopa.atlassian.net/wiki/spaces/PN/pages/1783628166/SRS+Picchi+di+recapito+microservizio+ritardatore+-+Fase+2#Tabella-pn-PaperDeliveriesCounter) su cui l’algoritmo: 1. salve le coppie Province~UnifiedDeliveryDriver (prefisso chiave EXCEED~) per le quali esistono delle eccedenze. 2. legge il numero di spedizioni che devono essere escluse dal check dei limiti del mittente per la coppa ProductType~Province (prefisso chiave EXCLUDE~). L’algoritmo si aspetta che la tabella sia già  - eventualmente - valorizzata per il caso 2 (righe con prefisso EXCLUDE~.. | operaz. IMPORT_DATA per quanto riguarda RS e secondo tentativi. |
| **deliveryDateDayOfWeek**                 | Intero che indica il giorno della settimana su cui l'algoritmo di pianificazione deve partire (default = 1, cioè lunedì).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | ---                                                             |



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
 