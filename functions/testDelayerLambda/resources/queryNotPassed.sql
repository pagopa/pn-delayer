SELECT *
FROM (
    SELECT DISTINCT requestId, notificationSentAt, prepareRequestDate, productType, senderPaId, province, cap, attempt, iun
    FROM "<PAPER_DELIVERY_JSON_VIEW>"
    WHERE p_year = '<YYYY>'
      AND p_month = '<MM>'
      AND p_day = '<DD>'
      AND pk = '<YYYY-MM-DD-NEXT-WEEK>~EVALUATE_SENDER_LIMIT'

    INTERSECT

    SELECT DISTINCT requestId, notificationSentAt, prepareRequestDate, productType, senderPaId, province, cap, attempt, iun
    FROM "<PAPER_DELIVERY_JSON_VIEW>"
    WHERE <QUERY_CONDITION_Q1>
);