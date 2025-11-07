'use strict';
const { persistRecord } = require('./dynamo');

/**
 * Extracts and persists records from an order.
 */
async function extractDataFromOrder(order, fileKey) {
    const records = buildRecordsFromOrder(order, fileKey);
    if (records.length > 0) {
        await persistRecord(records, fileKey);
    }

    return records;
}

/**
 * Builds all records from an order.
 */
function buildRecordsFromOrder(order, fileKey) {
    const records = [];
    const pk = getFirstDayOfMonth(order.periodo_riferimento);
    order.prodotti.forEach(product => {
        product.varianti.forEach(variant => {
            records.push(
                ...buildRecordsForVariant({
                    pk,
                    entityId: order.idEnte,
                    productId: product.id,
                    variant,
                    fileKey,
                    lastUpdateOrder: order.last_update,
                    createdAt: new Date().toISOString()
                })
            );
        });
    });
    return records;
}

/**
 * Builds aggregated and regional records for a variant.
 */
function buildRecordsForVariant({ pk, entityId, productId, variant, fileKey, lastUpdateOrder, createdAt }) {
    return [
        buildAggregatedRecord(pk, entityId, productId, variant, fileKey, lastUpdateOrder, createdAt),
        ...buildRegionalRecords(pk, entityId, productId, variant, fileKey, lastUpdateOrder, createdAt)
    ];
}

/**
 * Creates the aggregated record for a variant.
 */
function buildAggregatedRecord(pk, entityId, productId, variant, fileKey, lastUpdateOrder, createdAt) {
    return {
        pk,
        sk: `${entityId}_${productId}_${variant.codice}`,
        value: variant.valore_totale,
        fileKey,
        lastUpdateOrder,
        createdAt
    };
}

/**
 * Creates the regional records for a variant.
 */
function buildRegionalRecords(pk, entityId, productId, variant, fileKey, lastUpdateOrder, createdAt) {
    if (!variant.distribuzione?.regionale?.length) return [];
    return variant.distribuzione.regionale.map(regionDetail => ({
        pk,
        sk: `${entityId}_${productId}_${variant.codice}_${regionDetail.regione}`,
        value: regionDetail.valore,
        fileKey,
        lastUpdateOrder,
        createdAt
    }));
}

function getFirstDayOfMonth(dateString) {
    const [month, year] = dateString.split('-');
    return `${year}-${month}-01`;
}

module.exports = { extractDataFromOrder };
