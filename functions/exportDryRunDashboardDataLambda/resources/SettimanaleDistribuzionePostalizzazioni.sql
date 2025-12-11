WITH total AS (
-- spedizioni totale da elaborare
    SELECT
    COUNT(DISTINCT requestId) AS spedizioni,
    COUNT(DISTINCT IF(productType = 'RS', requestId, NULL)) AS rs,
    COUNT(DISTINCT IF(attempt='1', requestId, NULL)) AS secondi_tentativi
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q1>
),
passed AS (
    -- spedizioni che verranno elaborate giornalmente nella settimana in input 
    -- (al netto della capacità di stampa)
    SELECT DISTINCT requestId
    FROM pn_delayer_paper_delivery_json_view
    WHERE p_year='<YYYY>'
      AND p_month='<MM>'
      AND p_day='<DD>'
      AND pk='<YYYY-MM-DD>~EVALUATE_PRINT_CAPACITY'
),
not_passed AS (
    -- spedizioni che vanno alla settimana successiva che erano presenti 
    -- nella settimana corrente
    SELECT DISTINCT requestId, attempt, productType
    FROM pn_delayer_paper_delivery_json_view
    WHERE p_year='<YYYY>'
      AND p_month='<MM>'
      AND p_day='<DD>'
      AND pk='<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'
    
    INTERSECT
    
    SELECT DISTINCT requestId, attempt, productType
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q1>
)
SELECT
    '<YYYY-MM-DD>' AS settimana_elaborazione,
    CAST((SELECT spedizioni FROM total) AS INT) AS tot_spedizioni,
    CAST((SELECT rs FROM total) AS INT) AS totali_rs,
    CAST((SELECT secondi_tentativi FROM total) AS INT) AS tot_secondi_tentativi,
    CAST((SELECT COUNT(*) FROM passed) AS INT) AS tot_passed,
    CAST((SELECT COUNT(DISTINCT requestId) FROM not_passed) AS INT) AS tot_not_passed,
    CAST((SELECT COUNT(DISTINCT requestId) FROM not_passed WHERE attempt='1') AS INT) AS tot_not_passed_sec_tent,
    CAST((SELECT COUNT(DISTINCT requestId) FROM not_passed WHERE productType='RS') AS INT) AS tot_not_passed_rs;