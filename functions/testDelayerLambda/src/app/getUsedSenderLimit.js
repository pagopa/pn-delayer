"use strict";
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");

const GSI_NAME = "deliveryDate-province-index";
const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

/**
 * GET_USED_SENDER_LIMIT operation
 * Supporta:
 *  - Object: { deliveryDate, province, lastEvaluatedKey?, pk?, table}
 */
async function getUsedSenderLimit(params = {}) {

    let deliveryDate, province, lastEvaluatedKey, pk, table;
    ({ deliveryDate, province, lastEvaluatedKey, pk, table} = params);

    if (!deliveryDate || (!province && !pk) || !table) {
        throw new Error("Parameters must include deliveryDate, (province or pk) and table");
    }

    if (pk) {
        const getParams = {
            TableName: table,
            Key: {
                pk: pk,
                deliveryDate: deliveryDate,
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
      TableName: table,
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

module.exports = { getUsedSenderLimit };