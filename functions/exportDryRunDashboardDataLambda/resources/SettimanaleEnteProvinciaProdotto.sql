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
limite AS (
    SELECT 
        pk,
        senderLimit
    FROM pn_paper_delivery_used_sender_limit_json_view
     WHERE p_year = '<YYYY-NEXT-WEEK>'
      AND p_month = '<MM-NEXT-WEEK>'
      AND p_day = '<DD-NEXT-WEEK>'
      AND deliveryDate = '<YYYY-MM-DD>'
),
total AS (
-- spedizioni totale da elaborare
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS spedizioni,
        COUNT(DISTINCT IF(productType = 'RS', requestId, NULL)) AS rs,
        COUNT(DISTINCT IF(attempt='1', requestId, NULL)) AS secondi_tentativi
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q1> 
    GROUP BY senderPaId, province, productType
),
passed AS (
    -- spedizioni che verranno elaborate giornalmente nella settimana in input (al netto della capacità di stampa)
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS tot_passed
    FROM pn_delayer_paper_delivery_json_view
    WHERE p_year='<YYYY>'
      AND p_month='<MM>'
      AND p_day='<DD>'
      AND pk='<YYYY-MM-DD>~EVALUATE_PRINT_CAPACITY'
    GROUP BY senderPaId, province, productType
),
not_passed AS (
    -- spedizioni che vanno alla settimana successiva che erano presenti nella settimana corrente
    SELECT
        senderPaId,
        province,
        productType,
        COUNT(DISTINCT requestId) AS tot_not_passed,
        COUNT(DISTINCT IF(attempt = '1', requestId, NULL)) AS tot_not_passed_sec_tent
    FROM (
        SELECT DISTINCT requestId, attempt, productType, senderPaId, province
        FROM pn_delayer_paper_delivery_json_view
        WHERE p_year='<YYYY>'
        AND p_month='<MM>'
        AND p_day='<DD>'
        AND pk='<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'
    
    INTERSECT
    
    SELECT DISTINCT requestId, attempt, productType, senderPaId, province
        FROM pn_delayer_paper_delivery_json_view
        WHERE <QUERY_CONDITION_Q1> 
    )
    GROUP BY senderPaId, province, productType
)

SELECT
    COALESCE(c.paId, t.senderPaId, p.senderPaId, n.senderPaId) AS paId,
    COALESCE(c.province, t.province, p.province, n.province) AS province,
    COALESCE(c.productType, t.productType, p.productType, n.productType) AS productType,
    c.deliveryDate,
    COALESCE(TRY_CAST(c.commessaMensileRegionale AS INTEGER), 0) AS commessaMensileRegionale,
    COALESCE(TRY_CAST(c.commessaMensileProvinciale AS INTEGER), 0) AS commessaMensileProvinciale,
    COALESCE(TRY_CAST(c.commessaSettimanaleProvinciale AS INTEGER), 0) AS commessaSettimanaleProvinciale,
    COALESCE(TRY_CAST(l.senderLimit AS INTEGER), 0) AS limite_garantito,
    COALESCE(TRY_CAST(t.spedizioni AS INTEGER), 0) AS spedizioni,
    COALESCE(TRY_CAST(t.rs AS INTEGER), 0) AS rs,
    COALESCE(TRY_CAST(t.secondi_tentativi AS INTEGER), 0) AS secondi_tentativi,
    COALESCE(TRY_CAST(p.tot_passed AS INTEGER), 0) AS tot_passed,
    COALESCE(TRY_CAST(n.tot_not_passed AS INTEGER), 0) AS tot_not_passed,
    COALESCE(TRY_CAST(n.tot_not_passed_sec_tent AS INTEGER), 0) AS tot_not_passed_sec_tent
FROM commessa c
FULL JOIN limite l
    ON c.pk = l.pk
FULL JOIN total t
    ON c.paId = t.senderPaId
   AND c.province = t.province
   AND c.productType = t.productType
FULL JOIN passed p
    ON c.paId = p.senderPaId
   AND c.province = p.province
   AND c.productType = p.productType
FULL JOIN not_passed n
    ON c.paId = n.senderPaId
   AND c.province = n.province
   AND c.productType = n.productType;
