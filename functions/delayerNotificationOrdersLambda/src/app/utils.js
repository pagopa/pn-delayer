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
    const pk = getFirstDayOfMonth(order.reference_period);

    order.products.forEach(product => {
        product.variants.forEach(variant => {
            records.push(
                ...buildRecordsForVariant({
                    pk,
                    entityId: order.entityId,
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
        sk: `${entityId}_${productId}_${variant.code}`,
        value: variant.total_value,
        fileKey,
        lastUpdateOrder,
        createdAt
    };
}

/**
 * Creates the regional records for a variant.
 */
function buildRegionalRecords(pk, entityId, productId, variant, fileKey, lastUpdateOrder, createdAt) {
    if (!variant.distribution?.regional?.length) return [];

    return variant.distribution.regional.map(regionDetail => ({
        pk,
        sk: `${entityId}_${productId}_${variant.code}_${regionDetail.region}`,
        value: regionDetail.value,
        fileKey,
        lastUpdateOrder,
        createdAt
    }));
}

/**
 * Returns the first day of the month from MM-YYYY format.
 */
function getFirstDayOfMonth(dateString) {
    const [month, year] = dateString.split('-');
    return `${year}-${month}-01`;
}

module.exports = { extractDataFromOrder };
