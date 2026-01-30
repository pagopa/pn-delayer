WITH 
-- 1. Driver/Geokey saturi (uguale alla tua logica)
SaturatedDrivers AS (
    SELECT DISTINCT 
        unifiedDeliveryDriver, 
        geokey
    FROM pn_paper_delivery_driver_used_capacities_json_view 
    WHERE p_year='<YYYY>' 
      AND p_month='<MM>' 
      AND p_day='<DD>' 
      AND deliveryDate='<YYYY-MM-DD>'
      AND usedCapacity = declaredCapacity
),

-- 2. Calcoliamo l'INTERSECT dei requestId in una CTE separata
ValidRequestIds AS (
    SELECT 
        requestId,
        productType, 
        province, 
        unifiedDeliveryDriver
    FROM pn_delayer_paper_delivery_json_view
    WHERE p_year='<YYYY>' 
      AND p_month='<MM>' 
      AND p_day='<DD>'
      AND pk='<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'
    
    INTERSECT
    
    SELECT 
        requestId,
        productType, 
        province, 
        unifiedDeliveryDriver
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q1>
)

-- 3. Query finale: Filtra, Unisce e Conta
SELECT 
    t2.unifiedDeliveryDriver,
    t2.geokey,
    t1.productType, 
    t1.province, 
    t1.unifiedDeliveryDriver,
    '<YYYY-MM-DD>' AS deliveryDate,
    COUNT(DISTINCT t1.requestId) AS num_requests
FROM pn_delayer_paper_delivery_json_view t1
-- Filtriamo t1 usando solo gli ID che hanno superato l'intersect
INNER JOIN ValidRequestIds v 
    ON t1.requestId = v.requestId
-- Uniamo i driver saturi per capire "chi" sta bloccando "cosa"
INNER JOIN SaturatedDrivers t2
    ON t1.unifiedDeliveryDriver = t2.unifiedDeliveryDriver
    AND (t1.cap = t2.geokey OR t1.province = t2.geokey)
WHERE t1.p_year='<YYYY>'
  AND t1.p_month='<MM>'
  AND t1.p_day='<DD>'
  AND t1.pk='<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'
GROUP BY 
    t2.unifiedDeliveryDriver, 
    t2.geokey,
    t1.productType, 
    t1.province, 
    t1.unifiedDeliveryDriver;