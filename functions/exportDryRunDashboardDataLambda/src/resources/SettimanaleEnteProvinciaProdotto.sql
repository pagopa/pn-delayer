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
)
SELECT DISTINCT
    c.pk,
    c.paId,
    c.province,
    c.productType,
    c.deliveryDate,
    c.commessaMensileRegionale,
    c.commessaMensileProvinciale,
    c.commessaSettimanaleProvinciale,
    l.senderLimit AS limite_garantito
FROM commessa c
LEFT JOIN limite l
    ON c.pk = l.pk;
