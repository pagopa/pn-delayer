'use strict';
const {
    startOfMonth,
    endOfMonth,
    eachWeekOfInterval,
    getDaysInMonth,
    differenceInCalendarDays,
    addDays,
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
 * @param fileKey fileKey of Safe Storage
 * @returns {Promise<Array<Object>>}        List of weekly‑granularity records.
 */
async function calculateWeeklyEstimates(commessa, getProvinceDistribution, fileKey) {
    const { segments, daysInMonth } = getMonthContext(commessa.periodo_riferimento);
    const partialStart = segments.filter(s => s.weekType === 'PARTIAL_START').length;
    const partialEnd   = segments.filter(s => s.weekType === 'PARTIAL_END').length;

    console.debug(
        `[ALGO] Month context – segments: ${segments.length}, partialStart: ${partialStart}, partialEnd: ${partialEnd}, daysInMonth: ${daysInMonth}`
      );
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
                    segments,
                });

                results.push(...provinceRecords);

                if (provinceRecords.length > 0) {
                    await persistWeeklyEstimates(provinceRecords, fileKey);
                }
            }
        }
    }

    console.info(`[ALGO] ▶︎ End – total records generated: ${results.length}`);
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
    const weeks = eachWeekOfInterval({ start: startMonth, end: endMonth }, { weekStartsOn: 1 });

    const segments = [];
    for (const weekStart of weeks) {
        const weekEnd = addDays(weekStart, 6);

        // overlap of this week with the month
        const overlapStart = weekStart < startMonth ? startMonth : weekStart;
        const overlapEnd   = weekEnd   > endMonth   ? endMonth   : weekEnd;

        const daysInWeekInMonth = differenceInCalendarDays(overlapEnd, overlapStart) + 1;
        if (daysInWeekInMonth <= 0) continue; // should not happen

        let weekType = 'FULL';
        if (overlapStart.getTime() === weekStart.getTime() && daysInWeekInMonth < 7) {
            weekType = 'PARTIAL_END'; // last week: starts Mon, ends before Sun
        } else if (overlapStart.getTime() !== weekStart.getTime()) {
            weekType = 'PARTIAL_START'; // first week: starts after Mon
        }

        segments.push({ weekStart, daysInWeekInMonth, weekType });
    }

    return {
        segments,
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
                                  segments
                              }) {
    const records = [];
    const monthlyRegionalEstimate = regionale.valore;

    for (const { province: provinceSigla, percentageDistribution: percentage } of provinceDistribution) {
        const perc = percentage ?? 100;          // default a 100%
        const monthlyProvEstimate = (monthlyRegionalEstimate * perc) / 100;
        const dailyProvEstimate   = monthlyProvEstimate / daysInMonth;

        // full weeks inside the month
    for (const { weekStart, daysInWeekInMonth, weekType } of segments) {
        const weeklyProvEstimate = Math.round(dailyProvEstimate * daysInWeekInMonth);
        records.push(buildRecord({
            commessa,
            productType,
            provinceSigla,
            weekStart,
            weeklyEstimate: weeklyProvEstimate,
            monthlyEstimate: monthlyProvEstimate,
            originalEstimate: monthlyRegionalEstimate,
            weekType,
            daysInWeekInMonth
        }));
        }
    }

    return records;
}

function buildRecord({ commessa,
                         productType,
                         provinceSigla,
                         weekStart,
                         weeklyEstimate,
                         monthlyEstimate,
                         originalEstimate,
                         weekType,
                         daysInWeekInMonth }) {
    return {
        paId: commessa.idEnte,
        productType,
        province: provinceSigla,
        deliveryDate: formatISO(weekStart, { representation: 'date' }), // YYYY‑MM‑DD
        weeklyEstimate,
        monthlyEstimate,
        originalEstimate,
        lastUpdate: commessa.last_update,
        weekType,                 // "FULL" | "PARTIAL_START" | "PARTIAL_END"
        daysInWeekInMonth         // 7 for FULL, <7 for partials
    };
}

module.exports = { calculateWeeklyEstimates };
