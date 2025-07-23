'use strict';
const {
    startOfMonth,
    endOfMonth,
    eachWeekOfInterval,
    getDaysInMonth,
    differenceInCalendarDays,
    formatISO
} = require('date-fns');

const { persistWeeklyEstimates } = require('./dynamo');

/**
 * Calculate weekly provincial estimates starting from a monthly‑regional JSON “commessa”.
 * Split into small helpers to keep Cognitive Complexity < 15.
 *
 * @param {object}  commessa                Parsed JSON of the commessa.
 * @param {function(region:string):Promise<Array<{province:string, percentageDistribution:number}>>} getProvinceDistribution
 *                                          Async function returning province distribution for a region.
 * @returns {Promise<Array<Object>>}        List of weekly‑granularity records.
 */
async function calculateWeeklyEstimates(commessa, getProvinceDistribution) {
    const { weekNotInTheMonth, weeks, daysInMonth } = getMonthContext(commessa.periodoRiferimento);
    const results = [];

    const filteredProducts = commessa.prodotti.filter(prod => prod.id === "AR" || prod.id === "890");

    for (const prodotto of filteredProducts ?? []) {
        for (const variante of prodotto.varianti ?? []) {
            const regionals = variante.distribuzione?.regionale ?? [];

            for (const regionale of regionals) {
                const provinceDistribution = await getProvinceDistribution(regionale.regione);
                if (!provinceDistribution?.length) {
                    console.error(`No distribution configured for region "${regionale.regione}".`);
                    continue;
                }

                const provinceRecords = buildProvinceRecords({
                    provinceDistribution,
                    regionale,
                    productType: prodotto.id,
                    commessa,
                    daysInMonth,
                    weeks,
                    weekNotInTheMonth
                });

                results.push(...provinceRecords);

                if (provinceRecords.length > 0) {
                    await persistWeeklyEstimates(provinceRecords);
                }
            }
        }
    }

    return results;
}

/**
 * Compute month context (first day, ISO‑weeks and days in month) from “mm‑YYYY” string.
 * @param {string} periodoRiferimento e.g. "07-2025"
 */
function getMonthContext(periodoRiferimento) {
    const [monthStr, yearStr] = periodoRiferimento.split('-');
    const month = Number(monthStr) - 1; // JS months 0‑based
    const year = Number(yearStr);
    const startMonth = startOfMonth(new Date(Date.UTC(year, month, 1)));
    const endMonth   = endOfMonth(startMonth);

    //weeks contiene tutte le settimana del mese che iniziano di lunedì + eventualmente quella precedente
    const weeks      = eachWeekOfInterval({ start: startMonth, end: endMonth }, { weekStartsOn: 1 });

    const weekNotInTheMonth = weeks[0].getMonth() !== weeks[1].getMonth() ? weeks.shift() : null;


    return {
        weekNotInTheMonth,
        weeks,
        daysInMonth: getDaysInMonth(startMonth)
    };
}

/**
 * Build estimates for every province of a region.
 */
function buildProvinceRecords({
                                  provinceDistribution,
                                  regionale,
                                  productType,
                                  commessa,
                                  daysInMonth,
                                  weeks,
                                  weekNotInTheMonth
                              }) {
    const records = [];
    const monthlyRegionalEstimate = regionale.valore;

    for (const { province: provinceSigla, percentageDistribution: percentage } of provinceDistribution) {
        const perc = percentage ?? 100;          // default a 100%
        const monthlyProvEstimate = (monthlyRegionalEstimate * perc) / 100;
        const dailyProvEstimate   = monthlyProvEstimate / daysInMonth;
        const weeklyProvEstimate  = Math.round(dailyProvEstimate * 7);

        // full weeks inside the month
        for (const monday of weeks) {
            records.push(buildRecord({
                commessa,
                productType,
                provinceSigla,
                monday,
                weeklyEstimate: weeklyProvEstimate,
                monthlyEstimate: monthlyProvEstimate
            }));
        }

        // partial week spanning previous month
        if (weekNotInTheMonth) {
            const missingDays = differenceInCalendarDays(weeks[0], endOfMonth(weekNotInTheMonth)) - 1;
            const additionalEst   = dailyProvEstimate * missingDays;

            records.push(buildRecord({
                commessa,
                productType,
                provinceSigla,
                monday: weekNotInTheMonth,
                weeklyEstimate: Math.round(additionalEst),
                // monthlyEstimate: monthlyProvEstimate,
                isPartialWeek: true
            }));
        }
    }

    return records;
}

function buildRecord({
                         commessa,
                         productType,
                         provinceSigla,
                         monday,
                         weeklyEstimate,
                         monthlyEstimate,
                         isPartialWeek = false
                     }) {
    return {
        paId: commessa.idEnte,
        productType,
        province: provinceSigla,
        deliveryDate: formatISO(monday, { representation: 'date' }), // YYYY‑MM‑DD
        weeklyEstimate,
        monthlyEstimate,
        lastUpdate: commessa.lastUpdate,
        ...(isPartialWeek ? { isPartialWeek: true } : {})
    };
}

module.exports = { calculateWeeklyEstimates };
