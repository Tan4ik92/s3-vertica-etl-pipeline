DELETE FROM DWH.global_metrics
WHERE date_update = DATE '{{ target_date }}';

INSERT INTO DWH.global_metrics (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
SELECT
    DATE '{{ target_date }}' AS date_update,
    t.currency_code AS currency_from,
    SUM(
        CASE
            WHEN t.currency_code = 420
                THEN t.amount / 100.0
            ELSE
                (t.amount / 100.0) * c.currency_with_div
        END
    )::NUMERIC(18, 2) AS amount_total,
    COUNT(*) AS cnt_transactions,
    (
        COUNT(*)::NUMERIC(18, 4)
        / NULLIF(COUNT(DISTINCT t.account_number_from), 0)
    )::NUMERIC(18, 2) AS avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
FROM STAGING.transactions t
LEFT JOIN STAGING.currencies c
    ON c.currency_code = t.currency_code
   AND c.currency_code_with = 420
   AND c.date_update = DATE '{{ target_date }}'
WHERE t.transaction_dt::date = DATE '{{ target_date }}'
  AND t.status = 'done'
  AND t.account_number_from >= 0
  AND t.account_number_to >= 0
GROUP BY t.currency_code;