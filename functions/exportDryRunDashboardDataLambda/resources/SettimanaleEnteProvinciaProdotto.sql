WITH sender_limit_partitions AS (
    SELECT *
    FROM pn_paper_delivery_sender_limit_json_view
    WHERE <QUERY_CONDITION_Q2>
      AND deliveryDate = '<YYYY-MM-DD>'
),

sender_limit_latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY pk
            ORDER BY kinesis_dynamodb_ApproximateCreationDateTime DESC
        ) AS rn
    FROM sender_limit_partitions
),

commessa AS (
    SELECT
        paId,
        province,
        productType,
        deliveryDate,
        pk,
        originalEstimate AS commessaMensileRegionale,
        monthlyEstimate AS commessaMensileProvinciale,
        weeklyEstimate AS commessaSettimanaleProvinciale
    FROM sender_limit_latest
    WHERE rn = 1
),

-- Garantisce una sola riga per PA, provincia e prodotto
commessa_unica AS (
    SELECT
        paId,
        province,
        productType,
        MAX(TRY_CAST(commessaMensileRegionale AS INTEGER))
            AS commessaMensileRegionale,
        MAX(TRY_CAST(commessaMensileProvinciale AS DOUBLE))
            AS commessaMensileProvinciale,
        MAX(TRY_CAST(commessaSettimanaleProvinciale AS INTEGER))
            AS commessaSettimanaleProvinciale
    FROM commessa
    GROUP BY
        paId,
        province,
        productType
),

limite AS (
    SELECT
        paId,
        province,
        productType,
        senderLimit
    FROM pn_paper_delivery_used_sender_limit_json_view
    WHERE p_year = '<YYYY>'
      AND p_month = '<MM>'
      AND p_day = '<DD>'
      AND deliveryDate = '<YYYY-MM-DD-LAST-WEEK>'
),

-- Elimina le possibili duplicazioni dei limiti
limite_unico AS (
    SELECT
        paId,
        province,
        productType,
        MAX(TRY_CAST(senderLimit AS INTEGER)) AS limite_garantito
    FROM limite
    GROUP BY
        paId,
        province,
        productType
),

total AS (
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS spedizioni,
        COUNT(DISTINCT IF(
            productType = 'RS',
            requestId,
            NULL
        )) AS rs,
        COUNT(DISTINCT IF(
            attempt = '1',
            requestId,
            NULL
        )) AS secondi_tentativi
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q1>
    GROUP BY
        senderPaId,
        province,
        productType
),

passed AS (
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS tot_passed
    FROM pn_delayer_paper_delivery_json_view
    WHERE p_year = '<YYYY>'
      AND p_month = '<MM>'
      AND p_day = '<DD>'
      AND pk = '<YYYY-MM-DD>~EVALUATE_PRINT_CAPACITY'
    GROUP BY
        senderPaId,
        province,
        productType
),

not_passed AS (
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS tot_not_passed,
        COUNT(DISTINCT IF(
            attempt = '1',
            requestId,
            NULL
        )) AS tot_not_passed_sec_tent,
        COUNT(DISTINCT IF(
            productType = 'RS',
            requestId,
            NULL
        )) AS tot_not_passed_rs
    FROM (
        SELECT DISTINCT
            requestId,
            attempt,
            productType,
            senderPaId,
            province
        FROM pn_delayer_paper_delivery_json_view
        WHERE p_year = '<YYYY>'
          AND p_month = '<MM>'
          AND p_day = '<DD>'
          AND pk = '<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'

        INTERSECT

        SELECT DISTINCT
            requestId,
            attempt,
            productType,
            senderPaId,
            province
        FROM pn_delayer_paper_delivery_json_view
        WHERE <QUERY_CONDITION_Q1>
    ) AS matching_requests
    GROUP BY
        senderPaId,
        province,
        productType
),

in_prepare_fase_2 AS (
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS in_prepare_fase_2
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q3>
    GROUP BY
        senderPaId,
        province,
        productType
),

-- Costruisce tutte le combinazioni distinte
-- presenti nelle sei sorgenti
chiavi AS (
    SELECT paId, province, productType
    FROM commessa_unica

    UNION

    SELECT paId, province, productType
    FROM limite_unico

    UNION

    SELECT senderPaId AS paId, province, productType
    FROM total

    UNION

    SELECT senderPaId AS paId, province, productType
    FROM passed

    UNION

    SELECT senderPaId AS paId, province, productType
    FROM not_passed

    UNION

    SELECT senderPaId AS paId, province, productType
    FROM in_prepare_fase_2
)

SELECT
    k.paId,
    k.province,
    k.productType,

    '<YYYY-MM-DD>' AS deliveryDate,

    COALESCE(c.commessaMensileRegionale, 0)
        AS commessaMensileRegionale,

    COALESCE(c.commessaMensileProvinciale, 0)
        AS commessaMensileProvinciale,

    COALESCE(c.commessaSettimanaleProvinciale, 0)
        AS commessaSettimanaleProvinciale,

    COALESCE(l.limite_garantito, 0)
        AS limite_garantito,

    COALESCE(TRY_CAST(t.spedizioni AS INTEGER), 0)
        AS spedizioni,

    COALESCE(TRY_CAST(t.rs AS INTEGER), 0)
        AS rs,

    COALESCE(TRY_CAST(t.secondi_tentativi AS INTEGER), 0)
        AS secondi_tentativi,

    COALESCE(TRY_CAST(p.tot_passed AS INTEGER), 0)
        AS tot_passed,

    COALESCE(TRY_CAST(n.tot_not_passed AS INTEGER), 0)
        AS tot_not_passed,

    COALESCE(TRY_CAST(n.tot_not_passed_sec_tent AS INTEGER), 0)
        AS tot_not_passed_sec_tent,

    COALESCE(TRY_CAST(n.tot_not_passed_rs AS INTEGER), 0)
        AS tot_not_passed_rs,

    COALESCE(TRY_CAST(f.in_prepare_fase_2 AS INTEGER), 0)
        AS in_prepare_fase_2

FROM chiavi k

LEFT JOIN commessa_unica c
    ON k.paId IS NOT DISTINCT FROM c.paId
   AND k.province IS NOT DISTINCT FROM c.province
   AND k.productType IS NOT DISTINCT FROM c.productType

LEFT JOIN limite_unico l
    ON k.paId IS NOT DISTINCT FROM l.paId
   AND k.province IS NOT DISTINCT FROM l.province
   AND k.productType IS NOT DISTINCT FROM l.productType

LEFT JOIN total t
    ON k.paId IS NOT DISTINCT FROM t.senderPaId
   AND k.province IS NOT DISTINCT FROM t.province
   AND k.productType IS NOT DISTINCT FROM t.productType

LEFT JOIN passed p
    ON k.paId IS NOT DISTINCT FROM p.senderPaId
   AND k.province IS NOT DISTINCT FROM p.province
   AND k.productType IS NOT DISTINCT FROM p.productType

LEFT JOIN not_passed n
    ON k.paId IS NOT DISTINCT FROM n.senderPaId
   AND k.province IS NOT DISTINCT FROM n.province
   AND k.productType IS NOT DISTINCT FROM n.productType

LEFT JOIN in_prepare_fase_2 f
    ON k.paId IS NOT DISTINCT FROM f.senderPaId
   AND k.province IS NOT DISTINCT FROM f.province
   AND k.productType IS NOT DISTINCT FROM f.productType;