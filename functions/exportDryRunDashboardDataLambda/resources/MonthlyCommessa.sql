WITH date_config AS (
    SELECT 
        -- *** SEZIONE TEST ***
        -- Per testare: decommenta la riga sotto (es. oggi è Aprile, vuoi vedere Giugno? Metti Maggio)
        -- DATE '2026-03-25' AS execution_date 
        
        -- Per produzione:
        current_date AS execution_date
),
parametro_temporale AS (
    SELECT
        -- Calcoliamo il primo giorno del MESE SUCCESSIVO
        date_trunc('month', execution_date + INTERVAL '1' MONTH) AS next_month_date
    FROM date_config
),
final_params AS (
    SELECT
        format_datetime(next_month_date, 'yyyy') AS anno_riferimento,
        format_datetime(next_month_date, 'MM') AS mese_riferimento,
        format_datetime(next_month_date, 'yyyy-MM-dd') AS pk_riferimento
    FROM parametro_temporale
),
base AS (
    SELECT
        o.pk,
        o.sk,
        o.p_year,
        o.p_month,
        o.p_day,
        CAST(o.value AS INT) AS commessa,
        ROW_NUMBER() OVER (
            PARTITION BY o.pk, o.sk
            ORDER BY o.kinesis_dynamodb_ApproximateCreationDateTime DESC
        ) AS rn
    FROM pn_notification_orders_json_view o
    INNER JOIN final_params p ON o.p_year = p.anno_riferimento 
                              AND o.pk = p.pk_riferimento
    WHERE (length(o.sk) - length(replace(o.sk, '~', '')) = 3) or (o.sk like '%~digitale~%') or (o.sk like '%~INT')
),
sk_extracted AS (
    SELECT
        split_part(sk, '~', 1) AS senderpaid,
        -- Gestione dinamica del PRODOTTO
        CASE 
            WHEN sk LIKE '%~digitale~%' THEN split_part(sk, '~', 3)
            WHEN sk LIKE '%~INT' THEN 'RIR'
            ELSE split_part(sk, '~', 2)
        END AS prodotto,
        -- Gestione dinamica della REGIONE
        CASE 
            WHEN sk LIKE '%~digitale~%' or sk like '%~INT' THEN null
            ELSE split_part(sk, '~', 4)
        END AS regione,
        commessa,
        CAST(p_year || '-' || p_month || '-' || p_day AS DATE) AS data_commessa,
        format_datetime(CAST(pk AS TIMESTAMP), 'MMMM') AS mese_validita,
        rn
    FROM base
)
SELECT
    senderpaid,
    regione,
    prodotto,
    data_commessa AS "Data commessa",
    mese_validita AS "Mese validita",
    SUM(commessa) AS totale_volumi
FROM sk_extracted
WHERE rn = 1 
GROUP BY 1, 2, 3, 4, 5
ORDER BY senderpaid DESC