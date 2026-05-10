WITH date_context AS (
    SELECT 
        -- USA QUESTA RIGA PER IL TEST (inserisci la data che vuoi simulare)
        -- DATE '2026-05-01' AS base_date
        -- USA QUESTA RIGA PER LA PRODUZIONE (togli il commento e commenta quella sopra)
        current_date AS base_date
),
calculated_dates AS (
    SELECT 
        base_date - INTERVAL '1' DAY AS yesterday,
        date_trunc('week', base_date - INTERVAL '1' DAY) AS monday_of_yesterday
    FROM date_context
),
in_prepare_fase_2 AS (
    SELECT
        senderPaId,
        province,
        productType,
        -- Cast diretto a DATE per permettere l'Incremental Refresh di QuickSight
        CAST(substr(notificationSentAt, 1, 10) AS DATE) as notificationDate,
        COUNT(DISTINCT requestId) AS in_prepare_fase_2
    FROM pn_delayer_paper_delivery_json_view
    WHERE 
        p_year  = format_datetime((SELECT yesterday FROM calculated_dates), 'yyyy')
        AND p_month = format_datetime((SELECT yesterday FROM calculated_dates), 'MM')
        AND p_day   = format_datetime((SELECT yesterday FROM calculated_dates), 'dd') -- Nota: 'dd' minuscolo per il giorno del mese
        AND pk = format_datetime((SELECT monday_of_yesterday FROM calculated_dates), 'yyyy-MM-dd') || '~SENT_TO_PREPARE_PHASE_2'
    GROUP BY 
        senderPaId, 
        province, 
        productType, 
        substr(notificationSentAt, 1, 10)
)
SELECT * FROM in_prepare_fase_2