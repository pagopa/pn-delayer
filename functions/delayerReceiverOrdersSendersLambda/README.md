# pn-delayer-receiver-orders-senders Lambda

AWS Lambda (Node 20) that consumes **Safe Storage** “file ready” events, downloads the
**commessa** JSON uploaded by Portale Fatturazione, transforms monthly–regional estimates into
**weekly–provincial** ones (according to the algorithm defined in the SRS),
and persists them into DynamoDB.

## Folder structure

```
.
├── index.js               # Lambda entry‑point
├── package.json
└── src
    ├── app
    │   ├── algorithm.js   # Business algorithm implementation
    │   ├── dynamo.js      # DAO helpers for DynamoDB
    │   ├── eventHandler.js
    │   └── safeStorage.js # REST client for Safe Storage
    └── test
        └── algorithm.test.js
```

## Environment variables

| Name                | Description                                      |
|---------------------|--------------------------------------------------|
| `SAFE_STORAGE_URL`  | Base URL of Safe Storage REST API (e.g. `https://api.pn.pagopa.it`) |
| `CX_ID`             | Client identifier header value for Safe Storage  |
| `PROVINCE_TABLE`    | (Optional) Name of **pn-PaperChannelProvince** table |
| `LIMIT_TABLE`       | (Optional) Name of **pn-PaperDeliverySenderLimit** table |
| `COUNTERS_TABLE`    | (Optional) Name of **pn-PaperDeliveryCounters** table |

## Tests

```bash
npm install
npm test
```

Unit tests are written with **Jest** and mock external dependencies (`@aws-sdk/*`).

## Deployment

The Lambda can be deployed with AWS SAM, Serverless Framework or directly through
CloudFormation (`pn-delayer` stack).  
Ensure the execution role has permissions for:

* **DynamoDB** – `GetItem`, `Query`, `BatchWriteItem`, `UpdateItem` on the three tables
* **SQS** – `ReceiveMessage`, `DeleteMessage` on `pn-safestorage_to_delayer_orders_senders` queue
* **Secrets Manager / Parameter Store** – (if you store `CX_ID` there)

```yaml
# Example snippet (AWS SAM)
ReceiverOrdersSendersFunction:
  Type: AWS::Serverless::Function
  Properties:
    Runtime: nodejs20.x
    Handler: index.handler
    CodeUri: .
    Environment:
      Variables:
        SAFE_STORAGE_URL: https://api.pn.pagopa.it
        CX_ID: !Ref SafeStorageCxId
    Events:
      SafeStorageQueue:
        Type: SQS
        Properties:
          Queue: !GetAtt OrdersSendersQueue.Arn
```

## Algorithm (high‑level)

1. **Retrieve provinces for every region** using `pn-PaperChannelProvince` to get the
   percentage distribution.
2. **Convert monthly estimates to daily**, then multiply by 7 to obtain the weekly estimate.
3. **Generate all Mondays** in the reference month; insert one item per Monday.
4. If the first Monday is **after the 1st**, calculate a **partial week** for the previous Monday
   and update (or create) its record.
5. **Aggregate total estimates** per product+province in `pn-PaperDeliveryCounters`.

See `src/app/algorithm.js` for the exact implementation.
