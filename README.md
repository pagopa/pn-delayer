# pn-delayer
Repository contenente i componenti realizzati per gestire in modo efficiente i picchi di recapito in modo tale da non appesantire i recapitisti sulla base di spedizioni ricevute dalle notifiche create dagli enti mittenti.

## Panoramica
Si compone di:

- AWS **Lambda**:
    - **pn-delayer-kinesisPaperDeliveryLambda**: gestisce la ricezione degli eventi Kinesis relativi alla prepare fase 1 e la scrittura sulla tabella `pn-PaperDeliveryHighPriority`
    - **pn-delayer-submitPaperDeliveryJobLambda**: Si occupa della submit dei Job di schedulazione spedizioni
    - **pn-delayerToPaperChannelLambda**: responsabile della lettura dalla tabella `pn-PaperDeliveryReadyToSend` e scrittura sulla coda `pn-delayer_to_paperchannel`.
    - **pn-delayerToPaperChannelRecoveryLambda**: Ha un comportamento analogo alla lambda precedente ma ha l'opzione di scegliere una data da usare come chiave primaria per la lettura sulla tabella `pn-PaperDeliveryReadyToSend`.
- Microservizio Spring Boot 3
    - **pn-delayer**: Job di schedulazione spedizioni sulla base delle capacità dei recapitisti. Dati in input `unifiedDeliveryDriver` e `province`, recupera tutti le richieste di spedizioni per quella copia, dalla tabella `pn-PaperDeliveryHighPriority`. \
                      Per ogni record recuperato, valuta la se il recapitista ha capacità sia per provincia che per CAP (recuperando i record dalle tabelle `pn-PaperDeliveryDriverCapacities` e `pn-PaperDeliveryDriverUsedCapacities`). \
                      Se vi è capacità sia per provincia che per CAP, allora il microservizio Java sposta il record nella tabella `pn-PaperDeliveryReadyToSend` e lo elimina dalla tabella `pn-PaperDeliveryHighPriority`.

### Architettura
![Architettura.png](Architettura.svg)
https://excalidraw.com/#json=PAngScUbYCWtPF1bxyJJh,EoCtsGXgj2bPM9hr7rgh3A

## Pn-delayer-kinesisPaperDeliveryLambda
### Responsabilità
- Lettura degli eventi del Kinesis Data Stream `pn-delayer_inputs` relativi alla prepare fase 1
- Inserimento di tali eventi sulla tabella `Pn-PaperDeliveryHighPriority`

### Configurazione
| Variabile Ambiente         | Descrizione                                                                  | Obbligatorio | Default |
|----------------------------|------------------------------------------------------------------------------|--------------|---------|
| `REGION`                   |                                                                              | Sì           |         |
| `HIGH_PRIORITY_TABLE_NAME` | Nome della tabella DynamoDB per la gestione delle notifiche ad alta priorità | Sì           |         |
| `BATCH_SIZE`               | Dimensione massima del batch per l'elaborazione delle notifiche              | No           | 25      |


## Pn-delayer-submitPaperDeliveryJobLambda
### Responsabilità
- recupero di tutti gli `unifiedDeliveryDriver` e delle province associate
- sottomissione di un numero di `AWS Job Array` pari al numero degli `unifiedDeliveryDriver` recuperati, ognuno avente un numero di `child job` pari al numero di province associate al recapitista.

### Configurazione
| Variabile Ambiente                 | Descrizione                                                                                           | Obbligatorio |
|------------------------------------|-------------------------------------------------------------------------------------------------------|--------------|
| `REGION`                           |                                                                                                       | Sì           |
| `JOB_QUEUE`                        | Arn della coda di Job pn-delayer-job-queue                                                            | Sì           |
| `JOB_DEFINITION`                   | Arn della Job definition pn-delayer-job-definition                                                    | Sì           |
| `JOB_INPUT_PARAMETER`              | SSM parameter name for SubmitPaperDeliveryJob input tuples                                            | Sì           |  
| `JOB_INPUT_DRIVER_ENV_NAME`        | Nome della env per l'unifiedDeliveryDriver input del job (PN_DELAYER_JOBINPUT_UNIFIEDDELIVERYDRIVER)  | Sì           |  
| `JOB_INPUT_PROVINCE_LIST_ENV_NAME` | Nome della env per la lista di province input del job (PN_DELAYER_JOBINPUT_PROVINCELIST)              | Sì           |  


## Pn-delayer
### Responsabilità
- recupero spedizioni dalla tabella pn-PaperDeliveryHighPriority
- verifica della capacità per le coppie recapitista-provincia e recapitista-cap nella settimana in cui la spedizione dovrebbe essere schedulata
- calcolo della deliveryDate per ogni spedizione basata sul cut-off e sull'intervallo di schedulazione
- inserimento degli item pronti per essere inviati alla prepare fase 2 nella tabella pn-paperDeliveryReadyToSend

### Configurazione
| Variabile Ambiente                                          | Descrizione                                                           | Obbligatorio | Default |
|-------------------------------------------------------------|-----------------------------------------------------------------------|--------------|---------|
| `PN_DELAYER_DAO_PAPERDELIVERYDRIVERCAPACITIESTABLENAME`     | tabella per le capacità settimanale dei recapitisti                   | Si           |         |
| `PN_DELAYER_DAO_PAPERDELIVERYDRIVERUSEDCAPACITIESTABLENAME` | tabella per il numero di spedizioni settimanali effettuate            | Si           |         |
| `PN_DELAYER_DAO_PAPERDELIVERYHIGHPRIORITYTABLENAME`         | tabella per le spedizioni da affidare                                 | Si           |         |
| `PN_DELAYER_DAO_PAPERDELIVERYREADYTOSENDTABLENAME`          | tabella per le spedizioni pronte per la prepare fase 2                | Si           |         |
| `PN_DELAYER_HIGHPRIORITYQUERYLIMIT`                         | Limite per la query sulla tabella Pn-PaperDeliveryHighPriority        | Si           | 1000    |
| `PN_DELAYER_DELIVERYDATEDAYOFWEEK`                          | Primo giorno della settimana di cut-off (0=Dom, 1=Lun, ...)           | Si           | 1       |
| `PN_DELAYER_DELIVERYDATEINTERVAL`                           | Intervallo di schedulazione delle spedizioni all'interno del cut-off  | Si           | 1d      |
| `AWS_REGIONCODE`                                            |                                                                       | Si           |         |
| `PN_DELAYER_JOBINPUT_UNIFIEDDELIVERYDRIVER`                 | UnifiedDeliveryDriver di input per il JOB                             | Si           |         |
| `PN_DELAYER_JOBINPUT_PROVINCELIST`                          | Province relative all'unifiedDeliveryDriver di input per il JOB       | Si           |         |
| `PN_DELAYER_PAPERDELIVERYCUTOFFDURATION`                    | Durata del cut-off (es. 7 - settimanale, 0 - cutoff spento)           | Si           | 7d      |


## Pn-delayerToPaperChannelLambda
Lambda schedulata dalla regola event bridge `pn-delayerToPaperChannelScheduleRule`.
### Responsabilità
- recupero delle spedizioni, schedulate nella data di esecuzione della lambda, dalla tabella `Pn-PaperDeliveryReadyToSend`
- invio delle spedizioni recuperate sulla coda `pn-delayer_to_paperchannel`

### Configurazione
| Variabile Ambiente                    | Descrizione                                                        | Obbligatorio |Default |
|---------------------------------------|--------------------------------------------------------------------|--------------|--------|
| `REGION`                              |                                                                    | Sì           |        |
| `PAPERDELIVERYREADYTOSEND_TABLENAME`  | Tabella per le spedizioni pronte per la prepare fase 2             | Sì           |        |
| `PAPERDELIVERYREADYTOSEND_QUERYLIMIT` | Limite per la query sulla tabella Pn-PaperDeliveryReadyToSend      | No           | 1000   |
| `DELAYERTOPAPERCHANNEL_QUEUEURL`      | URL della coda per la prepare fase 2 (pn-delayer_to_paperchannel)  | Sì           |        |


## Pn-delayerToPaperChannelRecoveryLambda
Lambda, schedulata dalla regola event bridge `pn--DelayerToPaperChannelRecoveryScheduleRule`.
### Responsabilità
- recupero delle spedizioni, schedulate nella data definita dalla env `PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE` o se non presente nel giorno precedente alla data di esecuzione della lambda, dalla tabella `Pn-PaperDeliveryReadyToSend`
- invio delle spedizioni recuperate sulla coda `pn-delayer_to_paperchannel`

### Configurazione
| Variabile Ambiente                              | Descrizione                                                     | Obbligatorio | Default |
|-------------------------------------------------|-----------------------------------------------------------------|--------------|---------|
| `REGION`                                        | Regione AWS in cui è deployato il microservizio                 | Sì           |         |
| `PAPERDELIVERYREADYTOSEND_TABLENAME`            | Nome della tabella DynamoDB per le notifiche pronte per l'invio | Sì           |         |
| `PAPERDELIVERYREADYTOSEND_QUERYLIMIT`           | Limite massimo di elementi da recuperare per query              | No           | 1000    |
| `DELAYERTOPAPERCHANNEL_QUEUEURL`                | URL della coda SQS per l'invio delle notifiche ai recapitisti   | Sì           |         |
| `PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE` | Data di recupero per le notifiche non inviate                   | No           | ''      |


## Testing in locale

### Prerequisiti
1. JDK 21 installato in locale
2. Docker/Podman avviato con container di Localstack (puoi utilizzare il Docker Compose di [Localdev] https://github.com/pagopa/pn-localdev)

I dettagli sui test di integrazione e le procedure di testing sono disponibili in [README_TEST.md](./README_TEST.md).


 
