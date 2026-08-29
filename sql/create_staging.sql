DROP TABLE IF EXISTS STAGING.transactions CASCADE;
DROP TABLE IF EXISTS STAGING.currencies CASCADE;
DROP TABLE IF EXISTS STAGING.srv_wf_settings CASCADE;

CREATE TABLE STAGING.transactions (
    operation_id UUID NOT NULL,
    account_number_from INT NOT NULL,
    account_number_to INT NOT NULL,
    currency_code INT NOT NULL,
    country VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    amount INT NOT NULL,
    transaction_dt TIMESTAMP NOT NULL
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;

CREATE TABLE STAGING.currencies (
    currency_code INT NOT NULL,
    currency_code_with INT NOT NULL,
    date_update DATE NOT NULL,
    currency_with_div NUMERIC(18, 6) NOT NULL
)
ORDER BY date_update, currency_code, currency_code_with
SEGMENTED BY HASH(date_update, currency_code) ALL NODES;


CREATE TABLE STAGING.srv_wf_settings (
    workflow_key VARCHAR(100) NOT NULL,
    workflow_settings VARCHAR(1000) NOT NULL,
    updated_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
ORDER BY workflow_key
SEGMENTED BY HASH(workflow_key) ALL NODES;