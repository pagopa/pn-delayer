WITH date_context AS (
    -- USA QUESTA RIGA PER IL TEST (inserisci la data che vuoi simulare)
    -- SELECT DATE '2026-05-01' AS base_date
    -- USA QUESTA RIGA PER LA PRODUZIONE (togli il commento e commenta quella sopra)
    SELECT current_date - INTERVAL '1' DAY AS base_date
),
timeline_events AS (
    SELECT 
        dynamodb_keys_iun, 
        category, 
        details_physicalAddress_zip, 
        CASE 
            WHEN details_serviceLevel = 'REGISTERED_LETTER_890' THEN '890'
            WHEN details_serviceLevel = 'AR_REGISTERED_LETTER' THEN 'AR'
            ELSE details_serviceLevel 
        END AS details_serviceLevel,
        details_physicaladdress_foreignstate,
        notificationsentat, 
        timelineelementid,
        paid,
        timestamp
    FROM pn_timelines_json_view 
    WHERE p_year = format_datetime((SELECT base_date FROM date_context), 'yyyy') 
      AND p_month = format_datetime((SELECT base_date FROM date_context), 'MM')
      AND p_day   = format_datetime((SELECT base_date FROM date_context), 'dd')
      AND category IN ('PREPARE_DIGITAL_DOMICILE', 'PREPARE_ANALOG_DOMICILE', 'REQUEST_ACCEPTED')
),
filtered_events AS (
    SELECT *
    FROM timeline_events
    WHERE category = 'REQUEST_ACCEPTED' 
       OR (category != 'REQUEST_ACCEPTED' AND timelineelementid LIKE '%ATTEMPT_0%')
)
SELECT dynamodb_keys_iun AS iun, 
       category, 
        details_physicalAddress_zip AS zip, 
        details_serviceLevel AS product,
        details_physicaladdress_foreignstate AS state,
        notificationsentat, 
        paid,
        timestamp
        FROM filtered_events
        