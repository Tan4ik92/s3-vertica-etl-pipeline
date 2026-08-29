DROP TABLE IF EXISTS DWH.global_metrics CASCADE;

CREATE TABLE DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from INT NOT NULL,
    amount_total NUMERIC(18, 2) NOT NULL,
    cnt_transactions INT NOT NULL,
    avg_transactions_per_account NUMERIC(18, 2) NOT NULL,
    cnt_accounts_make_transactions INT NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;