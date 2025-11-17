'use strict';
const { persistOrderRecords } = require('./dynamo');
const {LocalDate} = require('@js-joda/core');

/**
 * Extracts and persists records from an order.
 */
async function extractDataFromOrder(order, fileKey) {
    const records = buildRecordsFromOrder(order, fileKey);
    if (records.length > 0) {
        await persistOrderRecords(records, fileKey);
    }

    return records;
}

/**
 * Builds all records from an order.
 */
function buildRecordsFromOrder(order, fileKey) {
    const createdAt = new Date().toISOString();
    const records = [];
    if (!order || !order.prodotti || !order.idEnte || !order.periodo_riferimento) {
      return records;
    }
    const pk = getFirstDayOfMonth(order.periodo_riferimento);
    order.prodotti.map(product => {
        product.varianti.forEach(variante => {
            records.push(
                ...buildRecords(order.idEnte, product.id, variante,product)
            );
        });
    });
    return records.map(record => {
        return {
            ...record,
            pk,
            fileKey,
            lastUpdateOrder: order.last_update,
            createdAt
        };
    });
}

/**
 * Creates the aggregated record for a variant.
 */
function buildRecords(paId, productId,variante,product) {
    let records = []
    records.push({
        sk: `${paId}~${productId}`,
        value: product.valore_totale
    });
    records.push( {
        sk: `${paId}~${productId}~${variante.codice}`,
        value: variante.valore_totale
    });
    if (!variante.distribuzione?.regionale?.length) return records;
    records.push( ...buildRegionalRecords(paId, productId, variante));
    return records;
}

/**
 * Creates the regional records for a variant.
 */
function buildRegionalRecords(paId, productId, variante) {
    return variante.distribuzione.regionale.map(regionDetail => ({
           sk: `${paId}~${productId}~${variante.codice}~${regionDetail.regione}`,
           value: regionDetail.valore
       }));
}

function getFirstDayOfMonth(dateString) {
    const [month, year] = dateString.split('-');
    return LocalDate.of(parseInt(year), parseInt(month), 1).toString();
}

module.exports = { extractDataFromOrder };
