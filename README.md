# S3 → Vertica ETL Pipeline

A batch data pipeline that loads financial transactions and currency rates from S3-compatible object storage into Vertica and builds a daily analytics mart.

## Architecture

```text
Yandex Object Storage / S3
        ↓
Apache Airflow
        ↓
Vertica STAGING
        ↓
Vertica DWH.global_metrics
```

## What the pipeline does

- Runs daily and supports Airflow backfills.
- Reads currency history and multiple transaction batches from S3.
- Filters data by business date, normalizes types, sorts records and removes duplicates.
- Reloads each daily partition safely, making retries idempotent.
- Converts transaction amounts to a common currency.
- Calculates daily turnover, transaction count, unique active accounts and average transactions per account.
- Keeps credentials outside source code by using Airflow Connections.

## Technologies

Python, SQL, Apache Airflow, Vertica, Pandas, boto3, Yandex Object Storage/S3.

## Repository structure

```text
dags/
  1_data_import.py
  2_datamart_update.py
sql/
  create_staging.sql
  create_dwh.sql
  global_metrics.sql
```

## Configuration

Create the following Airflow Connections:

- `s3_conn`: S3 endpoint and credentials in Connection Extra.
- `vertica_conn`: Vertica host, port, database, username and password.

Optional environment variables:

```text
S3_BUCKET_NAME
VERTICA_STG_SCHEMA
VERTICA_DWH_SCHEMA
```

No credentials are stored in this repository.
