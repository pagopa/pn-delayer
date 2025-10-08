'use strict';
const assert = require('assert');
const fs = require("fs");
const path = require("path");
const { calculateWeeklyEstimates } = require('../app/algorithm');

const { mockClient } = require("aws-sdk-client-mock");
const { DynamoDBDocumentClient, BatchWriteCommand, UpdateCommand, QueryCommand} = require("@aws-sdk/lib-dynamodb");
const ddbMock = mockClient(DynamoDBDocumentClient);
ddbMock.on(QueryCommand).resolves({ Items: [] });
ddbMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} });
ddbMock.on(UpdateCommand).resolves({});

const provider = async region => {
    if (region === 'Lombardia') {
        return [
            { province: 'MI', percentageDistribution: 60 },
            { province: 'BG', percentageDistribution: 40 }
        ];
    }
    if (region === 'Veneto') {
        return [
            { province: 'VE', percentageDistribution: 40 },
            { province: 'VR', percentageDistribution: 60 }
        ];
    }
    if (region === 'Piemonte') {
        return [
            { province: 'TO', percentageDistribution: 60 },
            { province: 'AL', percentageDistribution: 40 }
        ];
    }
    return [];
};

describe('calculateWeeklyEstimates', () => {

    beforeEach(() => {
        ddbMock.resetHistory();
        ddbMock.reset();

        // Default: tutte le send rispondono OK
        ddbMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} });
        ddbMock.on(UpdateCommand).resolves({});
        ddbMock.on(QueryCommand).resolves({ Items: [] });
    });

    it('should compute weekly estimates for February 2025', async () => {
        const sampleCommessaPath = path.join(__dirname, "Modulo_Commessa_v4.json");
        const sampleCommessa = JSON.parse(fs.readFileSync(sampleCommessaPath, "utf8"));
        const fileKey = 'fileKeySafe';
        const estimates = await calculateWeeklyEstimates(sampleCommessa, provider, fileKey);
        // February 2025 starts on Saturday, so first Monday is 2025-02-03
        // expect 4 Mondays + previous partial week = 5 weeks
        const weeksInMonth = 4;
        const partialWeek = 1;
        const uniqueWeeks = new Set(estimates.map(e => e.deliveryDate));
        assert.strictEqual(uniqueWeeks.size, weeksInMonth + partialWeek);
        // Check one sample: Milano AR product
        const miArFeb = estimates.find(
            e => e.province === 'MI' && e.productType === 'AR' && e.deliveryDate === '2025-02-03' && e.weekType === 'FULL'
        );

        const miArJanLastWeek = estimates.find(
            e => e.province === 'MI' && e.productType === 'AR' && e.deliveryDate === '2025-01-27' && e.weekType === 'PARTIAL_START'
        );

        const miArFebLast = estimates.find(
                    e => e.province === 'MI' && e.productType === 'AR' && e.deliveryDate === '2025-02-24' && e.weekType === 'PARTIAL_END'
                );

        // Lombardia AR 1000 mensile -> Milano 60% di 1000 -> 600 -> 600:28 giorni di feb = 21,4285714 -> x 7 giorni -> 150
        assert.strictEqual(miArFeb.weeklyEstimate, 150);

        // 21,4285714 x 2 giorni di febbraio -> 42,85 -> 43
        assert.strictEqual(miArJanLastWeek.weeklyEstimate, 43);
        assert.strictEqual(miArFebLast.weeklyEstimate, 107);
    });
});
