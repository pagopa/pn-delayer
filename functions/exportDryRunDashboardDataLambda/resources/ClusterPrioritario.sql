WITH input_ranked AS (
    SELECT
        requestId,
        senderPaId,
        province,
        cap,
        UPPER(productType) AS productType,
        COALESCE(TRY_CAST(attempt AS INTEGER), 0) AS attempt,
        UPPER(COALESCE(communicationType, 'LEGAL')) AS communicationType,
        notificationSentAt,
        LOWER(COALESCE(delayed, 'false')) = 'true' AS delayed_flag,
        LOWER(COALESCE(skipSenderLimit, 'false')) = 'true' AS skip_flag,
        ROW_NUMBER() OVER (
            PARTITION BY requestId, pk
            ORDER BY kinesis_dynamodb_ApproximateCreationDateTime DESC
        ) AS rn
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q1>
),
input AS (
    SELECT *
    FROM input_ranked
    WHERE rn = 1
      AND communicationType = 'LEGAL'
      AND productType IN ('AR', '890')
      AND attempt = 0
),
weekly_states_ranked AS (
    SELECT
        requestId,
        pk,
        workflowStep,
        unifiedDeliveryDriver,
        kinesis_dynamodb_ApproximateCreationDateTime,
        ROW_NUMBER() OVER (
            PARTITION BY requestId, pk
            ORDER BY kinesis_dynamodb_ApproximateCreationDateTime DESC
        ) AS rn
    FROM pn_delayer_paper_delivery_json_view
    WHERE p_year = '<YYYY>'
      AND p_month = '<MM>'
      AND p_day = '<DD>'
      AND pk IN (
          '<YYYY-MM-DD>~EVALUATE_DRIVER_CAPACITY',
          '<YYYY-MM-DD>~EVALUATE_RESIDUAL_CAPACITY',
          '<YYYY-MM-DD>~EVALUATE_PRINT_CAPACITY',
          '<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'
      )
),
weekly_states AS (
    SELECT *
    FROM weekly_states_ranked
    WHERE rn = 1
),
sent_to_phase_two AS (
    SELECT DISTINCT requestId
    FROM pn_delayer_paper_delivery_json_view
    WHERE <QUERY_CONDITION_Q3>
),
state_flags AS (
    SELECT
        requestId,
        MAX(CASE WHEN pk = '<YYYY-MM-DD>~EVALUATE_DRIVER_CAPACITY' THEN 1 ELSE 0 END) AS in_driver,
        MAX(CASE WHEN pk = '<YYYY-MM-DD>~EVALUATE_RESIDUAL_CAPACITY' THEN 1 ELSE 0 END) AS in_residual,
        MAX(CASE WHEN pk = '<YYYY-MM-DD>~EVALUATE_PRINT_CAPACITY' THEN 1 ELSE 0 END) AS in_print,
        MAX(CASE WHEN pk = '<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT' THEN 1 ELSE 0 END) AS postponed,
        MAX(unifiedDeliveryDriver) AS unifiedDeliveryDriver
    FROM weekly_states
    GROUP BY requestId
),
classified AS (
    SELECT
        DATE '<YYYY-MM-DD>' AS deliveryWeek,
        i.requestId,
        i.senderPaId,
        i.province,
        i.productType,
        i.cap,
        i.skip_flag,
        COALESCE(s.unifiedDeliveryDriver, 'NON_ASSEGNATO') AS unifiedDeliveryDriver,
        CASE
            WHEN i.delayed_flag THEN 'DELAYED'
            ELSE 'NOT_DELAYED'
        END AS delayedProfile,
        COALESCE(s.in_print, 0) AS reachedPrint,
        CASE WHEN p.requestId IS NOT NULL THEN 1 ELSE 0 END AS sentToPhaseTwo,
        COALESCE(s.postponed, 0) AS postponed,
        CASE
            WHEN COALESCE(s.postponed, 0) = 1 AND COALESCE(s.in_print, 0) = 1
                THEN 'RINVIO_STAMPA'
            WHEN COALESCE(s.postponed, 0) = 1
                 AND COALESCE(s.in_print, 0) = 0
                 AND (COALESCE(s.in_driver, 0) = 1 OR COALESCE(s.in_residual, 0) = 1)
                THEN 'RINVIO_RECAPITO'
            WHEN COALESCE(s.postponed, 0) = 1 THEN 'RINVIO_NON_CLASSIFICATO'
            ELSE NULL
        END AS inferredPostponementCause
    FROM input i
    LEFT JOIN state_flags s ON i.requestId = s.requestId
    LEFT JOIN sent_to_phase_two p ON i.requestId = p.requestId
)
SELECT
    deliveryWeek,
    province,
    cap,
    productType,
    senderPaId,
    unifiedDeliveryDriver,
    delayedProfile,
    skip_flag,
    COUNT(DISTINCT requestId) AS priorityInput,
    COUNT(DISTINCT IF(reachedPrint = 1, requestId, NULL)) AS reachedPrint,
    COUNT(DISTINCT IF(sentToPhaseTwo = 1, requestId, NULL)) AS sentToPhaseTwo,
    COUNT(DISTINCT IF(postponed = 1, requestId, NULL)) AS postponed,
    COUNT(DISTINCT IF(inferredPostponementCause = 'RINVIO_STAMPA', requestId, NULL)) AS postponedForPrint,
    COUNT(DISTINCT IF(inferredPostponementCause = 'RINVIO_RECAPITO', requestId, NULL)) AS postponedForDelivery,
    COUNT(DISTINCT IF(inferredPostponementCause = 'RINVIO_NON_CLASSIFICATO', requestId, NULL)) AS postponedUnclassified,
    ROUND(
        100.0 * COUNT(DISTINCT IF(reachedPrint = 1, requestId, NULL))
        / NULLIF(COUNT(DISTINCT requestId), 0),
        2
    ) AS reachedPrintRatePct,
    ROUND(
        100.0 * COUNT(DISTINCT IF(sentToPhaseTwo = 1, requestId, NULL))
        / NULLIF(COUNT(DISTINCT requestId), 0),
        2
    ) AS sentToPhaseTwoRatePct,
    ROUND(
        100.0 * COUNT(DISTINCT IF(postponed = 1, requestId, NULL))
        / NULLIF(COUNT(DISTINCT requestId), 0),
        2
    ) AS postponementRatePct
FROM classified
GROUP BY
    deliveryWeek,
    province,
    cap,
    productType,
    senderPaId,
    unifiedDeliveryDriver,
    delayedProfile,
    skip_flag
ORDER BY
    cap,
    province,
    productType,
    senderPaId,
    delayedProfile;