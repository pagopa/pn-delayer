# delayerToPaperChannelLambda

Lambda AWS Node.js 20 che finalizza lo step di capacita di stampa del delayer.

La Lambda e invocata dalla Step Function `static/stepfunctions/definitions/DelayerToPaperChannelStateMachine.json` e lavora sulle spedizioni gia presenti in DynamoDB nello step `EVALUATE_PRINT_CAPACITY`.

## Responsabilita

- Spostare verso `SENT_TO_PREPARE_PHASE_2` le spedizioni che rientrano nella capacita di stampa giornaliera e settimanale.
- Spostare verso `EVALUATE_SENDER_LIMIT` della settimana successiva le spedizioni eccedenti la capacita di stampa settimanale.
- Mantenere la paginazione del processamento tramite `lastEvaluatedKeyPhase2` e `lastEvaluatedKeyNextWeek`, che la Step Function salva nella tabella contatori.

La Lambda non cancella le righe originali: crea nuove righe nella stessa tabella `PaperDelivery` con un nuovo workflow step.

## Entry point

```text
index.handler -> src/app/eventHandler.handleEvent
```

## Flusso della Step Function

La state machine `DelayerToPaperChannelStateMachine` esegue questi passi principali:

1. Invoca la Lambda di pre-run `${PreRunAlgorithmLambdaName}` per calcolare la `deliveryWeek`.
2. Legge dalla tabella `PaperDeliveryCounter` il contatore con chiave:

   ```text
   pk = PRINT
   sk = <deliveryWeek>
   ```

3. Se il contatore non esiste termina con successo (`NoItemToProcess`).
4. Mappa il contatore in due blocchi:

   - `fixed`: valori statici del contatore per l'esecuzione, come capacita, numero spedizioni e contatori gia consolidati.
   - `variable`: stato mutabile della singola esecuzione, come last evaluated key e contatori incrementali.

5. Avvia due branch paralleli che invocano questa Lambda:

   | Branch | `processType` | Scopo |
   |--------|---------------|-------|
   | `SendPrintCapacityExceedToNextWeek` | `SEND_TO_NEXT_WEEK` | Sposta alla settimana successiva le spedizioni oltre la capacita settimanale. |
   | `SendPaperDeliveryToPreparePhase2` | `SEND_TO_PHASE_2` | Sposta in prepare phase 2 le spedizioni stampabili. |

6. Aggiorna il contatore DynamoDB con:

   - `lastEvaluatedKeyPhase2`
   - `lastEvaluatedKeyNextWeek`
   - `stopSendToPhaseTwo`
   - incremento di `sentToPhaseTwo`
   - incremento di `sentToNextWeek`
   - incremento o reset di `dailyExecutionCounter`

## Payload ricevuto dalla Lambda

La Step Function invoca la Lambda con un payload di questo tipo:

```json
{
  "executionDate": "2026-05-18T09:30:00Z",
  "paperDeliveryTableName": "pn-DelayerPaperDelivery",
  "processType": "SEND_TO_PHASE_2",
  "fixed": {
    "dailyExecutionCounter": 0,
    "dailyExecutions": 10,
    "dailyPrintCapacity": 1000,
    "numberOfShipments": 5000,
    "pk": "PRINT",
    "sentToNextWeek": 0,
    "sentToPhaseTwo": 0,
    "sk": "2026-05-18",
    "weeklyPrintCapacity": 7000
  },
  "variable": {
    "lastEvaluatedKeyNextWeek": {},
    "lastEvaluatedKeyPhase2": {},
    "lastExecution": false,
    "sendToNextStepCounter": 0,
    "sendToNextWeekCounter": 0,
    "stopSendToPhaseTwo": false
  }
}
```

`processType` puo assumere solo questi valori:

| Valore | Descrizione |
|--------|-------------|
| `SEND_TO_PHASE_2` | Processa le spedizioni da inviare allo step `SENT_TO_PREPARE_PHASE_2`. |
| `SEND_TO_NEXT_WEEK` | Processa le spedizioni da ripianificare nello step `EVALUATE_SENDER_LIMIT` della settimana successiva. |

Se `processType` manca o ha un valore diverso, la Lambda termina con errore.

## Input della Step Function

La Step Function si aspetta almeno i nomi delle tabelle DynamoDB:

```json
{
  "PAPERDELIVERY_TABLENAME": "pn-DelayerPaperDelivery",
  "PAPERDELIVERYCOUNTER_TABLENAME": "pn-PaperDeliveryCounters"
}
```

## Logica di processamento

### Calcolo della delivery week

La Lambda calcola la settimana di consegna partendo da `executionDate` e dalla variabile d'ambiente `PN_DELAYER_DELIVERYDATEDAYOFWEEK`.

Il valore risultante viene usato per interrogare la tabella `PaperDelivery` con partition key:

```text
<deliveryWeek>~EVALUATE_PRINT_CAPACITY
```

### Invio a prepare phase 2

Con `processType = SEND_TO_PHASE_2`, la Lambda:

1. Calcola `numberOfShipmentsPerExecution` come:

   ```text
   ceil(dailyPrintCapacity / dailyExecutions)
   ```

2. Calcola quante spedizioni puo ancora inviare nella singola esecuzione.
3. Verifica che ci sia capacita settimanale residua e che `stopSendToPhaseTwo` sia `false`.
4. Legge le spedizioni da `EVALUATE_PRINT_CAPACITY` in ordine crescente di sort key.
5. Inserisce le nuove righe con:

   ```text
   pk = <deliveryWeek>~SENT_TO_PREPARE_PHASE_2
   sk = <priority>~<date>~<requestId>
   workflowStep = SENT_TO_PREPARE_PHASE_2
   ```

### Invio alla settimana successiva

Con `processType = SEND_TO_NEXT_WEEK`, la Lambda:

1. Calcola l'eccedenza settimanale:

   ```text
   numberOfShipments - weeklyPrintCapacity
   ```

2. Sottrae quanto gia inviato alla settimana successiva (`sentToNextWeek`) e quanto processato nella corrente esecuzione (`sendToNextWeekCounter`).
3. Legge le spedizioni da `EVALUATE_PRINT_CAPACITY` in ordine decrescente di sort key.
4. Inserisce le nuove righe con:

   ```text
   pk = <deliveryWeek + 7 giorni>~EVALUATE_SENDER_LIMIT
   sk = <province>~<date>~<requestId>
   workflowStep = EVALUATE_SENDER_LIMIT
   ```

## Variabili d'ambiente

| Variabile | Descrizione | Default | Obbligatoria |
|-----------|-------------|---------|--------------|
| `PAPER_DELIVERY_QUERYLIMIT` | Limite massimo per ogni query DynamoDB sulla tabella `PaperDelivery`. | `1000` | No |
| `PN_DELAYER_DELIVERYDATEDAYOFWEEK` | Giorno della settimana usato per normalizzare `executionDate` alla delivery week. | `1` | No |
| `PN_MAXPAPERDELIVERIESFOREXECUTION` | Numero massimo di spedizioni processabili in una singola invocazione Lambda prima di restituire una last evaluated key alla Step Function. | - | Si |
| `REGION` | Regione AWS impostata dal template CloudFormation. | Regione runtime AWS | No |

I nomi delle tabelle non sono letti da variabili d'ambiente dalla Lambda: vengono passati nel payload dalla Step Function.

## Tabelle DynamoDB

### PaperDelivery

Tabella letta e scritta dalla Lambda.

- Lettura: query su `pk = <deliveryWeek>~EVALUATE_PRINT_CAPACITY`.
- Scrittura: `BatchWriteItem` di nuove righe per `SENT_TO_PREPARE_PHASE_2` o `EVALUATE_SENDER_LIMIT`.
- Le scritture sono divise in chunk da 25 item, come richiesto da DynamoDB BatchWrite.

### PaperDeliveryCounter

Tabella gestita direttamente dalla Step Function.

- La Step Function legge il contatore `PRINT~<deliveryWeek>`.
- La Step Function aggiorna contatori e last evaluated key al termine dei branch paralleli.

## Comandi locali

Eseguire i comandi dalla directory `functions/delayerToPaperChannelLambda`.

```bash
npm install
```

Test unitari:

```bash
npm test
```

Build dello zip Lambda:

```bash
npm run build
```

Test di integrazione, se configurato LocalStack e file `.env`:

```bash
npm run integrazione
```

## Note operative

- La Lambda e progettata per essere orchestrata dalla Step Function; l'invocazione manuale deve rispettare lo stesso contratto `fixed`/`variable`.
- `lastEvaluatedKeyPhase2` e `lastEvaluatedKeyNextWeek` viaggiano nel formato DynamoDB AttributeValue quando passano dalla Step Function alla Lambda.
- Per le spedizioni `RS` o con `attempt = 1`, la data nella sort key deriva da `prepareRequestDate`; per le altre spedizioni deriva da `notificationSentAt`.
- La Step Function limita il branch `SEND_TO_NEXT_WEEK` a 250000 item per esecuzione e il branch `SEND_TO_PHASE_2` alla capacita giornaliera e al numero di spedizioni per esecuzione.
