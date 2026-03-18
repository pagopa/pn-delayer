"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = "pn-PaperDeliverySenderLimit";
const GSI_NAME = "deliveryDateProvince-index";
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_SENDER_LIMIT operation
 * Supporta:
 *  - Object: { deliveryDate, province, lastEvaluatedKey?, pk? }
 */
async function getSenderLimit(params = {}) {

    let deliveryDate, province, lastEvaluatedKey, pk;
    ({ deliveryDate, province, lastEvaluatedKey, pk } = params);

    if (!deliveryDate || (!province && !pk)) {
        throw new Error("Parameters must include deliveryDate and (province or pk)");
    }

    if (pk) {
        const getParams = {
            TableName: TABLE_NAME,
            Key: {
                pk: pk,
                sk: deliveryDate,
            },
        };
        const command = new GetCommand(getParams);
        const { Item } = await docClient.send(command);
        if (!Item) {
            return { items: [] };
        }
        return { items: [Item] };
    }

    const limit = parseInt(process.env.PAPER_DELIVERY_QUERYLIMIT || '1000', 10);
    const queryParams = {
      TableName: TABLE_NAME,
      IndexName: GSI_NAME,
      KeyConditionExpression: "deliveryDate = :deliveryDate AND province = :province",
      ExpressionAttributeValues: {
        ":deliveryDate": deliveryDate,
        ":province": province,
      },
      Limit: limit,
    };

    if (lastEvaluatedKey) {
      queryParams.ExclusiveStartKey = lastEvaluatedKey;
    }

    const command = new QueryCommand(queryParams);


    const { Items, LastEvaluatedKey } = await docClient.send(command);
    if (!Items || Items.length === 0) {
        return { items: [] };
    }
    return { items: Items, lastEvaluatedKey: LastEvaluatedKey };
}

module.exports = { getSenderLimit };