## Test
**Premessa**   
Non è possibile eseguire in locale il JOB automaticamente partendo dalla sua invocazione dalla lambda `Pn-delayer-submitPaperDeliveryJobLambda` sarà quindi necessario avviare manualmente il microservizio `pn-delayer` in locale passando le env necessarie per l'esecuzione.

**Azioni preliminari**   
1. **Inserimento capacità dei recapitisti**  
Prima di eseguire un test completo è necessario lanciare lo script `src/test/resources/script/deliveryDriverCapacity/insert_capacity.js` per popolare la tabella `pn-PaperDeliveryDriverCapacities`.
    ``` 
    npm install
    node insert_capacity.js --coreProfile <coreProfile> --fileName <nome del file csv> --tenderId <id della gara> [--local] [--clearTable] [--noSSO]
    ```
    parametri:
    - **coreProfile** = obbligatorio - profilo dell'account core AWS
    - **fileName** = obbligatorio - nome del file csv contenente le province e i cap da popolare
    - **tenderId** = obbligatorio - id della gara
    - **local** = opzionale - booleano che indica se eseguire lo script localmente tramite localstack - default: false
    - **clearTable** = opzionale - booleano che indica se è necessario svuotare la tabella prima di eseguire lo script - default: false (pulizia della tabella consentita solo in locale)
    - **noSSO** = opzionale - booleano che indica se il profilo di AWS non è tramite login SSO

    Il file **Capacity_v1.csv** contiene le seguenti geoKey con capacità pari a 7:
      ``` 
      87100,
      CS,
      00118,
      RM
      20121,
      MI
      80122,
      80121,
      NA
      10121,
      TO
      09131,
      CA
      50121,
      50122,
      FI
      ```

    Il file **Capacity_load_test.csv** contiene le capacità per le province e i cap necessari al test di carico

    è possibile modificare tali file prima di eseguire lo script oppure crearne nuovi con le capacità desiderate, l'activation date from sarà popolato 
    con il giorno precedente all'esecuzione dello script per facilitare il test. se necessario è possibile inserire tale valore nel csv per avere una data custom.


2. **Inserimento capacità dei recapitisti**

    È necessario caricare manualmente i record sulle tabelle `pn-PaperDeliverySenderLimit` e `pn-PaperDeliveryCounters`.

- **Tabella `pn-PaperDeliverySenderLimit`**

  Esempio di record:
  ```
  {
    "pk": "senderPaId1~890~PE",
    "deliveryDate": "2025-07-28",
    "paId": "senderPaId1",
    "productType": "890",
    "province": "PE",
    "ttl": 1787926340,
    "weeklyEstimate": 0
  }
  ```

- **Tabella `pn-PaperDeliveryCounters`**  
    
   La chiave di ordinamento (**sk**) deve seguire il formato:

   ```
   SUM_ESTIMATES~<productType>~<province>~<timestamp>
   ```

   Esempio di record:
   ```
   {
     "pk": "2025-08-18",
     "sk": "SUM_ESTIMATES~890~AN~2025-01-01T00:00:00Z",
     "numberOfShipments": 2
   }
   ```

### Step
#### 1. Esecuzione della lambda `Pn-delayer-kinesisPaperDeliveryLambda`
**Premessa**
 - Prima di eseguire il test è necessario eseguire lo script `src/test/resources/script/generate_kinesis_event.sh` passando in input un json contenente una lista di messaggi provenienti dalla prepare fase 1 
   che si vuole utilizzare per generare la lista di eventi, se il nuovo json non dovesse essere passato allo script sarà utilizzato il file di `default.json` contenente 3 messaggi di
   esempio della prepare fase 1:

   esempio messages.json:
   ```
   [
    {
      requestId: string,
      iun: string,
      productType: string,
      senderPaId: string,
      recipientId: String,
      unifiedDeliveryDriver: string,
      tenderId: string,
      recipientNormalizedAddress:{
        cap: string,
        pr: string  
      }
    }
   ] 
   ```

   eseguire il comando:
   ```
    .\generate_kinesis_event.sh <filePath>
   ```

   parametri:
   - **filePath** = percorso del file json contenente la lista di payload dei messaggi della prepare fase 1 che si vogliono utilizzare

**Prerequisiti**:
- Aggiunger il file `.env` nella root della lambda:
  ```
  AWS_REGION=us-east-1
  REGION=us-east-1
  AWS_SECRET_ACCESS_KEY=PN-TEST
  AWS_ACCESS_KEY_ID=PN-TEST
  AWS_ENDPOINT_URL=http://localhost:4566
  KINESIS_PAPER_DELIVERY_TABLE_NAME=pn-DelayerPaperDelivery
  KINESIS_PAPER_DELIVERY_COUNTER_TABLE_NAME=pn-PaperDeliveryDriverCounters
  KINESIS_PAPER_DELIVERY_EVENT_TABLE_NAME=pn-PaperDeliveryKinesisEvent
  ```
- Modifica del file integration.test.js rimuovendo `.skip` alla riga 5

**Installazione delle dipendenze**:
- Spostarsi nella directory del modulo contenente la Lambda e installare le dipendenze:
  ```
  npm install
  ```
**Esecuzione del test**
- eseguire il comando
  ```
  npm run integrazione
  ```

#### 3. Test dell'algoritmo di pianificazione con i vari step `Pn-delayer`
N.B Poichè non è possibile eseguire l'algoritmo di pianificazione automatico lanciando le step function
sarà necessario avviare manualmente il microservizio `pn-delayer` in locale passando le env necessarie per l'esecuzione
di tutti e 3 gli step di pianificazione.

N.B.
- Per poter eseguire questo test in locale è necessaria la presenza di item relativi alla capacità sulla tabella
  pn-PaperDeliveryDriverCapacities basati sui cap e le province presenti nelle spedizioni della tabella Pn-DelayerPaperDelivery,
- la presenza dei contatori di RS e secondi tentativi, e delle somme delle stime dei mittenti sulla tabella
  pn-PaperDeliveryDriverCounters

### Step 1 - Evaluate Sender Limit - valutazione dei limiti dei mittenti

**Prerequisiti**:
- Popolare le seguenti env:
```
PN_DELAYER_PAPERDELIVERYPRIORITYPARAMETERNAME=/config/pn-delayer/paper-delivery-priority                                                                                  
PN_DELAYER_DAO_PAPERDELIVERYQUERYLIMIT=1000                                                                                        
PN_DELAYER_DAO_PAPERDELIVERYCOUNTERTABLENAME=pn-PaperDeliveryDriverCounters      
PN_DELAYER_DAO_PAPERDELIVERYSENDERLIMITTABLENAME=pn-PaperDeliverySenderLimit                                      
PN_DELAYER_DAO_PAPERDELIVERYUSEDSENDERLIMITTABLENAME=pn-PaperDeliveryUsedSenderLimit      
PN_DELAYER_DAO_PAPERDELIVERYDRIVERCAPACITIESTABLENAME=pn-PaperDeliveryDriverCapacities                              
PN_DELAYER_DAO_PAPERDELIVERYTABLENAME=pn-DelayerPaperDelivery                                                          
PN_DELAYER_EVALUATESENDERLIMITJOBINPUT_PROVINCE= <SIGLA DELLA PROVINCIA PER LA QUALE SI VUOLE LANCIARE IL JOB>                                                                                
PN_DELAYER_ACTUALTENDERI= <id della gara attiva>                                                                                                                          
PN_DELAYER_WORKFLOWSTEP=EVALUATE_SENDER_LIMIT
PN_DELAYER_PAPERCHANNELTENDERAPILAMBDAARN=arn:aws:lambda:eu-south-1:830192246553:function:pn-paper-channel-TenderAPI                                          
PN_DELAYER_DELIVERYDATEDAYOFWEEK=1
```

**Esecuzione del test**
- eseguire il run dell'applicativo

**Verifiche**
- Le spedizioni che sono nello step EVALUATE_SENDER_LIMIT che rientrano nei limiti garantiti ai mittenti
  dovranno essere inserite nello step EVALUATE_DRIVER_CAPACITY
- Le spedizioni che sono nello step EVALUATE_SENDER_LIMIT che rientrano nei limiti garantiti ai mittenti
  dovranno essere inserite nello step EVALUATE_RESIDUAL_CAPACITY

### Step 2 - Evaluate Driver Capacity - valutazione delle capacità di recapito

**Prerequisiti**:
- Popolare le seguenti env:
```
  PN_DELAYER_DAO_PAPERDELIVERYDRIVERCAPACITIESTABLENAME=pn-PaperDeliveryDriverCapacities                                                                                               
  PN_DELAYER_DAO_PAPERDELIVERYDRIVERUSEDCAPACITIESTABLENAME=pn-PaperDeliveryDriverUsedCapacities                                                                     
  PN_DELAYER_DAO_PAPERDELIVERYTABLENAME=pn-DelayerPaperDelivery                                                                                      
  PN_DELAYER_DELIVERYDATEDAYOFWEEK=1                                                                                                        
  PN_DELAYER_EVALUATEDRIVERCAPACITYJOBINPUT_UNIFIEDDELIVERYDRIVER= <recapitistas per il quale si vuole effettuare il test>                                         
  PN_DELAYER_EVALUATEDRIVERCAPACITYJOBINPUT_PROVINCELIST= <lista contenente la provincia per la quale si vuole effettuare il test                                                           
  PN_DELAYER_ACTUALTENDERID= <id della gara attiva>                                                                                                                                
  PN_DELAYER_WORKFLOWSTEP=EVALUATE_DRIVER_CAPACITY                                                                                                 
  PN_DELAYER_PRINTCAPACITYWEEKLYWORKINGDAYS=7                                                                                           
  PN_DELAYER_PRINTCOUNTERTTLDURATION=2d                                                                                          
  PN_DELAYER_DAO_PAPERDELIVERYQUERYLIMIT=1000                                                                                                         
  PN_DELAYER_DAO_PAPERDELIVERYCOUNTERTABLENAME=pn-PaperDeliveryDriverCounters
  AWS_BATCH_JOB_ARRAY_INDEX=0
```

**Esecuzione del test**
- eseguire il run dell'applicativo

**Verifiche**
- Le spedizioni che sono nello step EVALUATE_DRIVER_CAPACITY che rientrano nella capacità del recapitista
  dovranno essere inserite nello step EVALUATE_PRINT_CAPACITY
- Le spedizioni che sono nello step EVALUATE_DRIVER_CAPACITY che non rientrano nella capacità del recapitista
  dovranno essere inserite nello step EVALUATE_SENDER_LIMIT ma con deliveryDate alla settimana successiva

### Step 2 - Evaluate Residual Capacity - valutazione dei residui delle capacità di recapito

**Prerequisiti**:
- Popolare le seguenti env:
```
  PN_DELAYER_DAO_PAPERDELIVERYDRIVERCAPACITIESTABLENAME=pn-PaperDeliveryDriverCapacities                                                                                               
  PN_DELAYER_DAO_PAPERDELIVERYDRIVERUSEDCAPACITIESTABLENAME=pn-PaperDeliveryDriverUsedCapacities                                                                     
  PN_DELAYER_DAO_PAPERDELIVERYTABLENAME=pn-DelayerPaperDelivery                                                                                      
  PN_DELAYER_DELIVERYDATEDAYOFWEEK=1                                                                                                        
  PN_DELAYER_EVALUATERESIDUALCAPACITYJOBINPUT_UNIFIEDDELIVERYDRIVER= <recapitistas per il quale si vuole effettuare il test>                                         
  PN_DELAYER_EVALUATERESIDUALCAPACITYJOBINPUT_PROVINCELIST  = <lista contenente la provincia per la quale si vuole effettuare il test                                                           
  PN_DELAYER_ACTUALTENDERID= <id della gara attiva>                                                                                                                                
  PN_DELAYER_WORKFLOWSTEP=EVALUATE_DRIVER_CAPACITY                                                                                                 
  PN_DELAYER_PRINTCAPACITYWEEKLYWORKINGDAYS=7                                                                                           
  PN_DELAYER_PRINTCOUNTERTTLDURATION=2d                                                                                          
  PN_DELAYER_DAO_PAPERDELIVERYQUERYLIMIT=1000                                                                                                         
  PN_DELAYER_DAO_PAPERDELIVERYCOUNTERTABLENAME=pn-PaperDeliveryDriverCounters
  AWS_BATCH_JOB_ARRAY_INDEX=0
```

**Esecuzione del test**
- eseguire il run dell'applicativo

**Verifiche**
- Le spedizioni che sono nello step EVALUATE_SENDER_LIMIT che rientrano nei residui di capacità del recapitista
  dovranno essere inserite nello step EVALUATE_PRINT_CAPACITY
- Le spedizioni che sono nello step EVALUATE_SENDER_LIMIT che non rientrano nei residui di capacità del recapitista
  dovranno essere inserite nello step EVALUATE_SENDER_LIMIT ma con deliveryDate alla settimana successiva


#### 4. Esecuzione della lambda `Pn-delayerToPaperChannelLambda`
N.B. La lambda ha 2 possibili modalità di esecuzione:
- invio alla prepare fase 2
- invio degli eccessi della capacità di stampa allo step EVALUATE_SENDER_LIMIT della settimana successiva

Passaggi necessari per eseguire i test di integrazione:

**Prerequisiti**:
- Aggiungere il file `.env` nella root della lambda:
    ```
      AWS_REGION=us-east-1
      REGION=us-east-1
      AWS_SECRET_ACCESS_KEY=PN-TEST
      AWS_ACCESS_KEY_ID=PN-TEST
      AWS_ENDPOINT_URL=http://localhost:4566
      PAPER_DELIVERY_QUERYLIMIT=1000
      PN_DELAYER_DELIVERYDATEDAYOFWEEK=1
      PAPERDELIVERY_TABLENAME=pn-DelayerPaperDelivery
    ```

    - Popolare il file event.json con il payload necessario per eseguire la lambda in una delle 2 modalità:
    - Invio alla prepare fase 2
      ```
      {
        "processType":"SEND_TO_PHASE_2",
        "paperDeliveryTableName": "Pn-DelayerPaperDelivery",
        "input": {
            "sendToNextWeekCounter": "0",
            "lastEvaluatedKeyPhase2": {},
            "lastEvaluatedKeyNextWeek": {},
            "executionDate": "2025-01-01T00:00:00Z",
            "toNextWeekIncrementCounter": "0",
            "toNextStepIncrementCounter": "0"
        }
      }
      ```
    - Invio degli eccessi della capacità di stampa allo step EVALUATE_SENDER_LIMIT della settimana successiva
      ```
      {
        "processType":"SEND_TO_NEXT_WEEK",
        "paperDeliveryTableName": "Pn-DelayerPaperDelivery",
        "input": {
            "sendToNextWeekCounter": "0",
            "lastEvaluatedKeyPhase2": {},
            "lastEvaluatedKeyNextWeek": {},
            "executionDate": "2025-01-01T00:00:00Z",
            "toNextWeekIncrementCounter": "0",
            "toNextStepIncrementCounter": "0"
        }
      }
      ```
- Aggiungere il seguente script all'interno del file `package.json`:

- Modifica del file integration.test.js rimuovendo `.skip` alla riga 5

**Installazione delle dipendenze**:
- Spostarsi nella directory del modulo contenente la Lambda e installare le dipendenze:
  ```
  npm install
  ```
**Esecuzione del test**
- eseguire il comando
  ```
  npm run integrazione
  ```
**Verifiche**
- Accedere dalla console di localstack (https://app.localstack.cloud/inst/default/resources/dynamodb) alla tabella
- pn-DelayerPaperDelivery e verificare che le spedizioni che:
    - Le spedizioni che sono nello step EVALUATE_PRINT_CAPACITY che rientrano nella capacità di stampa giornaliera
      dovranno essere inserite nello step SENT_TO_PREPARE_PHASE_2
    - Le spedizioni che sono nello step EVALUATE_PRINT_CAPACITY che rientrano non nella capacità di stampa settimanale
      dovranno essere inserite nello step EVALUATE_SENDER_LIMIT ma con deliveryDate alla settimana successiva
- Verificare sempre sulla console di localstack (https://app.localstack.cloud/inst/default/resources/sqs) la presenza
  di un numero di messaggi sulla coda local-delayer_to_paperchannel pari al numero di spedizioni <= alla capacità di stampa
  giornaliera.
- il payload dei messaggi dovrà avere la seguente struttura:
  ```
  {
    requestId: <item.requestId>,
    iun: <item.iun>
  }
  ```


 
