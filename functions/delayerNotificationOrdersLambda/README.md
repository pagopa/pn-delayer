# pn-delayer-notification-orders Lambda

AWS Lambda (Node 20) designed to persist in the database the orders received from the Billing Portal in SEND, 
enabling the SEND team and cross-product teams to analyze sender behavior. Each order is represented by a JSON file for a sender–month pair, 
containing the entity’s estimates of analog (national and international) and digital notifications expected for the period, 
broken down by product (890, AR, PEC) and, for national analog products, also by region. 
Persisting all order data allows for more accurate monitoring and operational planning, 
verifying consistency between actual deposited notifications and the estimates declared by the entities.

## Folder structure

```
.
├── index.js               # Lambda entry‑point
├── package.json
└── src
    ├── app
    │   ├── utils.js   #  utility to extract record
    │   ├── dynamo.js      # DAO helpers for DynamoDB
    │   ├── eventHandler.js
    │   └── safeStorage.js # REST client for Safe Storage
    └── test
        └── algorithm.test.js
```

## Environment variables

| Name                  | Description                                                         |
|-----------------------|---------------------------------------------------------------------|
| `PN_SAFESTORAGE_URL`  | Base URL of Safe Storage REST API (e.g. `https://api.pn.pagopa.it`) |
| `PN_SAFESTORAGE_CXID` | Client identifier header value for Safe Storage                     |
| `NOTIFICATION-ORDER`  | (Optional) Name of **pn-NotificationOrders** table                  |

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
* **SQS** – `ReceiveMessage`, on `pn-safestorage_to_notification_orders` queue
* **Secrets Manager / Parameter Store** – (if you store `PN_SAFESTORAGE_CXID` there)

```yaml
# Example snippet (AWS SAM)
DelayerNotificationOrdersLambda:
  Type: AWS::Serverless::Function
  Properties:
    Runtime: nodejs20.x
    Handler: index.handler
    CodeUri: .
    Environment:
      Variables:
        PN_SAFESTORAGE_URL: https://api.pn.pagopa.it
        PN_SAFESTORAGE_CXID: !Ref SafeStorageCxId
    Events:
      SafeStorageQueue:
        Type: SQS
        Properties:
          Queue: !GetAtt SafeStorageToNotificationOrders.Arn
```
