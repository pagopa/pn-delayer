WITH 
-- 1. UsedCapacity eliminando eventuali duplicati, prendendo lo stream più recente
WeeklyUsedCapacities AS (
    SELECT *
    FROM (
        SELECT
            deliveryDate,
            unifiedDeliveryDriver, 
            geokey, 
            declaredCapacity, 
            usedCapacity,
            ROW_NUMBER() OVER (
                PARTITION BY unifiedDeliveryDriver, geokey 
                ORDER BY kinesis_dynamodb_ApproximateCreationDateTime DESC
            ) as rn
        FROM pn_paper_delivery_driver_used_capacities_json_view
        WHERE p_year='<YYYY>' AND p_month='<MM>' AND p_day='<DD>' 
          AND deliveryDate='<YYYY-MM-DD>'
    )
    WHERE rn = 1 
),

-- 2. Spedizioni del cutoff settimanale
ShipmentsInCutoff AS (
    SELECT requestId, cap, province
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q1>
),

-- 3. WeeklyUsedCapacities arricchiete con la provincia associata
CapacitiesWithProvince AS (
    SELECT distinct c.*, b.province from WeeklyUsedCapacities c LEFT JOIN ShipmentsInCutoff b on (c.geokey=b.cap or c.geokey=b.province)
),

-- 4. Calcolo Spedizioni in eccesso
Eccessi AS (
SELECT
        c.unifiedDeliveryDriver,
        c.geokey,
        -- Eccessi Totali
        COUNT(DISTINCT t1.requestId) AS eccessi_totali,

        -- Eccessi Prioritari: conta solo se skipSenderLimit è true
        COUNT(DISTINCT CASE
            WHEN t1.skipSenderLimit = 'true' THEN t1.requestId
        END) AS eccessi_prioritari,

        -- Eccessi Normali: conta solo se skipSenderLimit è false o nullo
        COUNT(DISTINCT CASE
            WHEN t1.skipSenderLimit = 'false' OR t1.skipSenderLimit IS NULL THEN t1.requestId
        END) AS eccessi_normali
    FROM pn_delayer_paper_delivery_json_view t1
    -- JOIN 1: Filtra t1 mantenendo solo i requestId presenti in ShipmentsInCutoff
    INNER JOIN ShipmentsInCutoff b ON t1.requestId = b.requestId
    -- JOIN 2: Lega il driver/geokey puliti
    INNER JOIN WeeklyUsedCapacities c 
        ON t1.unifiedDeliveryDriver = c.unifiedDeliveryDriver
        AND (t1.cap = c.geokey OR t1.province = c.geokey)
    WHERE t1.p_year='<YYYY>' 
      AND t1.p_month='<MM>' 
      AND t1.p_day='<DD>'
      AND t1.pk='<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'
      -- Importante: Contiamo gli eccessi SOLO se il driver è effettivamente saturo
      AND c.usedCapacity = c.declaredCapacity
    GROUP BY 
        c.unifiedDeliveryDriver, 
        c.geokey
)

-- 5. Output Finale
SELECT
    c.deliveryDate,
    c.unifiedDeliveryDriver, 
    c.geokey,
    c.province,
    c.declaredCapacity, 
    c.usedCapacity, 
    COALESCE(e.eccessi_totali, 0) AS eccessi,
    COALESCE(e.eccessi_normali, 0) AS eccessi_normali,
    COALESCE(e.eccessi_prioritari, 0) AS eccessi_prioritari
FROM CapacitiesWithProvince c
LEFT JOIN Eccessi e 
    ON c.unifiedDeliveryDriver = e.unifiedDeliveryDriver 
    AND c.geokey = e.geokey
ORDER BY e.eccessi_totali DESC, unifiedDeliveryDriver, geokey;