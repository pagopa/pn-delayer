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
  HIGH_PRIORITY_TABLE_NAME=pn-PaperDeliveryHighPriority
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
**Verifiche**
- Accedere dalla console di localstack (https://app.localstack.cloud/inst/default/resources/dynamodb) alla tabella pn-PaperDeliveryHighPriority e verificare
   l'inserimento degli item e il popolamento dei seguenti attributi:
  ```
  {
    pk: <event.unifiedDeliveryDriver>##<message.recipientNormalizedAddress.pr>
    createdAt: Instant.now,
    requestId: <message.requestId>,
    productType: <message.productType>,
    cap: <message.recipientNormalizedAddress.cap>,
    province: <message.recipientNormalizedAddress.pr>,
    senderPaId: <message.senderPaId>,
    recipientId: <message.recipientId>,
    unifieddeliveryDriver: <message.unifiedDeliveryDriver>,
    tenderId: <message.tenderId>,
    iun: <message.iun>
  } 
  ```
#### 2. Esecuzione della lambda `Pn-delayer-submitPaperDeliveryJobLambda`

Questa lambda non può essere testata in locale.

#### 3. Avvio del microservizio `Pn-delayer`

N.B.
- Per poter eseguire questo test in locale è necessaria la presenza di item relativi alla capacità sulla tabella pn-PaperDeliveryDriverCapacities basati sui cap e le province presenti nelle spedizioni della tabella Pn-PaperDeliveryHighPriority.
- Se si vuole effettuare il test senza cut-off sarà necessario popolare la env `PN_DELAYER_PAPERDELIVERYCUTOFFDURATION` specificando il valore 0

**Prerequisiti**:
- Popolare le seguenti env:
  ```
  AWS_REGIONCODE=us-east-1
  AWS_SECRET_ACCESS_KEY=PN-TEST
  AWS_ACCESS_KEY_ID=PN-TEST
  AWS_ENDPOINT_URL=http://localhost:4566
  PAPERDELIVERYREADYTOSEND_TABLENAME=pn-PaperDeliveryReadyToSend
  PN_DELAYER_DAO_PAPERDELIVERYHIGHPRIORITYTABLENAME=Pn-PaperDeliveryHighPriority
  PN_DELAYER_DAO_PAPERDELIVERYDRIVERUSEDCAPACITIESTABLENAME=pn-PaperDeliveryDriverUsedCapacities
  PN_DELAYER_DAO_PAPERDELIVERYDRIVERCAPACITIESTABLENAME=pn-PaperDeliveryDriverCapacities
  DELAYERTOPAPERCHANNEL_QUEUEURL=http://localstack:4566/000000000000/local-delayer_to_paperchannel
  PN_DELAYER_JOBINPUT_UNIFIEDDELIVERYDRIVER=<specificare un unified deliver driver presente nelle spedizioni della tabella Pn-PaperDeliveryHighPriority>
  PN_DELAYER_JOBINPUT_PROVINCELIST=<specificare una lista di province (sigla) contenente necessariamente le province presenti nelle spedizioni della tabella Pn-PaperDeliveryHighPriority ma anche province per le quali non sono presenti spedizioni per testare i vari scenari>
  ```

**Esecuzione del test**
- eseguire il run dell'applicativo

**Verifiche**
- Le spedizioni presenti nella tabella pn-PaperDeliveryHighPriority per le quali è presente capacità dovranno essere spostate nella tabella pn-PaperDeliveryReadyToSend impostando la data di spedizione basata sulle env PN_DELAYER_DELIVERYDATEDAYOFWEEK, PN_DELAYER_DELIVERYDATEINTERVAL e PN_DELAYER_PAPERDELIVERYCUTOFFDURATION
- Le spedizioni presenti nella tabella pn-PaperDeliveryHighPriority per le quali non è presente capacità non dovranno subire modifiche e rimarranno sulla tabella fino a che non sarà possibile schedularle.

#### 4. Esecuzione della lambda `Pn-delayerToPaperChannelLambda`
N.B. il json event.json contiene un evento vuoto in quanto tale lambda è schedulata e non lavora con eventi specifici

Passaggi necessari per eseguire i test di integrazione:

**Prerequisiti**:
- Aggiungere il file `.env` nella root della lambda:
  ```
  AWS_REGION=us-east-1
  REGION=us-east-1
  AWS_SECRET_ACCESS_KEY=PN-TEST
  AWS_ACCESS_KEY_ID=PN-TEST
  AWS_ENDPOINT_URL=http://localhost:4566
  PAPERDELIVERYREADYTOSEND_TABLENAME=pn-PaperDeliveryReadyToSend
  DELAYERTOPAPERCHANNEL_QUEUEURL=http://localstack:4566/000000000000/local-delayer_to_paperchannel
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
- Accedere dalla console di localstack (https://app.localstack.cloud/inst/default/resources/dynamodb) alla tabella pn-PaperDeliveryReadyToSend e verificare che tutti gli item aventi pk(deliveryDate) pari alla data di esecuzione della lambda siano stati correttamente cancellati.
- Verificare sempre sulla console di localstack (https://app.localstack.cloud/inst/default/resources/sqs) la presenza di un numero di messaggi sulla coda local-delayer_to_paperchannel pari al numero di item cancellati. il payload dei messaggi dovrà avere la seguente struttura:
  ```
  {
    requestId: <item.requestId>,
    iun: <item.iun>,
    attemptRetry: 0
  }
  ```

#### 5. Esecuzione della lambda `Pn-delayerToPaperChannelRecoveryLambda`
N.B il json event.json contiene un evento vuoto in quanto tale lambda è schedulata e non lavora con eventi specifici
Per questa lambda è possibile simulare due scenari:
- env `PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE` popolata per indicare quali spedizioni recuperare e inviare alla fase due
- env `PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE` non popolata, per recuperare le spedizioni schedulate un giorno prima della data di esecuzione della lambda non correttamente elaborate dalla lambda `Pn-delayerToPaperChannelLambda`

Passaggi necessari per eseguire i test di integrazione:

**Prerequisiti**:
- Aggiungere il file `.env` nella root della lambda:
  ```
  AWS_REGION=us-east-1
  REGION=us-east-1
  AWS_SECRET_ACCESS_KEY=PN-TEST
  AWS_ACCESS_KEY_ID=PN-TEST
  AWS_ENDPOINT_URL=http://localhost:4566
  PAPERDELIVERYREADYTOSEND_TABLENAME=pn-PaperDeliveryReadyToSend
  DELAYERTOPAPERCHANNEL_QUEUEURL=http://localstack:4566/000000000000/local-delayer_to_paperchannel
  ?PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE=2025-01-01T00:00:00Z
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
- Accedere dalla console di localstack (https://app.localstack.cloud/inst/default/resources/dynamodb) alla tabella pn-PaperDeliveryReadyToSend e verificare che tutti gli item aventi pk(deliveryDate) pari a `PAPERDELIVERYREADYTOSEND_RECOVERYDELIVERYDATE` o a `Date.now() - 1d` siano stati correttamente cancellati.
- Verificare sempre sulla console di localstack (https://app.localstack.cloud/inst/default/resources/sqs) la presenza di un numero di messaggi sulla coda local-delayer_to_paperchannel pari al numero di item cancellati. il payload dei messaggi dovrà avere la seguente struttura:
  ```
  {
    requestId: <item.requestId>,
    iun: <item.iun>,
    attemptRetry: 0
  }
  ```



 
