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

-- Each ADD on numberOfShipment produces a new CDC record: keep only the latest one per key
used_sender_limit_latest AS (
    SELECT
        paId,
        province,
        productType,
        senderLimit,
        ROW_NUMBER() OVER (
            PARTITION BY pk, deliveryDate
            ORDER BY kinesis_dynamodb_ApproximateCreationDateTime DESC
        ) AS rn
    FROM pn_paper_delivery_used_sender_limit_json_view
    WHERE p_year = '<YYYY>'
      AND p_month = '<MM>'
      AND p_day = '<DD>'
      AND deliveryDate = '<YYYY-MM-DD-LAST-WEEK>'
),

limite AS (
    SELECT 
        paId,
        province,
        productType,
        senderLimit
    FROM used_sender_limit_latest
    WHERE rn = 1
),

total AS (
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS spedizioni,
        COUNT(DISTINCT IF(productType = 'RS', requestId, NULL)) AS rs,
        COUNT(DISTINCT IF(attempt = '1', requestId, NULL)) AS secondi_tentativi
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q1>
    GROUP BY senderPaId, province, productType
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
    GROUP BY senderPaId, province, productType
),

not_passed AS (
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS tot_not_passed,
        COUNT(DISTINCT IF(attempt = '1', requestId, NULL)) AS tot_not_passed_sec_tent,
        COUNT(DISTINCT IF(productType = 'RS', requestId, NULL)) AS tot_not_passed_rs
    FROM (
        SELECT DISTINCT requestId, attempt, productType, senderPaId, province
        FROM pn_delayer_paper_delivery_json_view
        WHERE p_year = '<YYYY>'
          AND p_month = '<MM>'
          AND p_day = '<DD>'
          AND pk = '<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'

        INTERSECT

        SELECT DISTINCT requestId, attempt, productType, senderPaId, province
        FROM pn_delayer_paper_delivery_json_view
        WHERE <QUERY_CONDITION_Q1>
    )
    GROUP BY senderPaId, province, productType
),

in_prepare_fase_2 AS (
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS in_prepare_fase_2
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q3>
    GROUP BY senderPaId, province, productType
),

chiavi AS (
    SELECT paId, province, productType FROM commessa
    UNION
    SELECT paId, province, productType FROM limite
    UNION
    SELECT senderPaId, province, productType FROM total
    UNION
    SELECT senderPaId, province, productType FROM passed
    UNION
    SELECT senderPaId, province, productType FROM not_passed
    UNION
    SELECT senderPaId, province, productType FROM in_prepare_fase_2
),

joined AS (
    SELECT
        k.paId,
        k.province,
        k.productType,

        c.deliveryDate,

        TRY_CAST(c.commessaMensileRegionale AS INTEGER) AS commessaMensileRegionale,
        TRY_CAST(c.commessaMensileProvinciale AS DOUBLE) AS commessaMensileProvinciale,
        TRY_CAST(c.commessaSettimanaleProvinciale AS INTEGER) AS commessaSettimanaleProvinciale,

        TRY_CAST(l.senderLimit AS INTEGER) AS limite_garantito,

        TRY_CAST(t.spedizioni AS INTEGER) AS spedizioni,
        TRY_CAST(t.rs AS INTEGER) AS rs,
        TRY_CAST(t.secondi_tentativi AS INTEGER) AS secondi_tentativi,

        TRY_CAST(p.tot_passed AS INTEGER) AS tot_passed,
        TRY_CAST(n.tot_not_passed AS INTEGER) AS tot_not_passed,
        TRY_CAST(n.tot_not_passed_sec_tent AS INTEGER) AS tot_not_passed_sec_tent,
        TRY_CAST(n.tot_not_passed_rs AS INTEGER) AS tot_not_passed_rs,
        TRY_CAST(f.in_prepare_fase_2 AS INTEGER) AS in_prepare_fase_2

    FROM chiavi k
    LEFT JOIN commessa c
        ON k.paId = c.paId
       AND k.province = c.province
       AND k.productType = c.productType
    LEFT JOIN limite l
        ON k.paId = l.paId
       AND k.province = l.province
       AND k.productType = l.productType
    LEFT JOIN total t
        ON k.paId = t.senderPaId
       AND k.province = t.province
       AND k.productType = t.productType
    LEFT JOIN passed p
        ON k.paId = p.senderPaId
       AND k.province = p.province
       AND k.productType = p.productType
    LEFT JOIN not_passed n
        ON k.paId = n.senderPaId
       AND k.province = n.province
       AND k.productType = n.productType
    LEFT JOIN in_prepare_fase_2 f
        ON k.paId = f.senderPaId
       AND k.province = f.province
       AND k.productType = f.productType
)

SELECT
    paId,
    province,
    productType,

    '<YYYY-MM-DD>' AS deliveryDate,

    COALESCE(MAX(commessaMensileRegionale), 0) AS commessaMensileRegionale,
    COALESCE(MAX(commessaMensileProvinciale), 0) AS commessaMensileProvinciale,
    COALESCE(MAX(commessaSettimanaleProvinciale), 0) AS commessaSettimanaleProvinciale,

    COALESCE(MAX(limite_garantito), 0) AS limite_garantito,

    COALESCE(SUM(spedizioni), 0) AS spedizioni,
    COALESCE(SUM(rs), 0) AS rs,
    COALESCE(SUM(secondi_tentativi), 0) AS secondi_tentativi,

    COALESCE(SUM(tot_passed), 0) AS tot_passed,
    COALESCE(SUM(tot_not_passed), 0) AS tot_not_passed,
    COALESCE(SUM(tot_not_passed_sec_tent), 0) AS tot_not_passed_sec_tent,
    COALESCE(SUM(tot_not_passed_rs), 0) AS tot_not_passed_rs,
    COALESCE(SUM(in_prepare_fase_2), 0) AS in_prepare_fase_2

FROM joined
GROUP BY
    paId,
    province,
    productType;
