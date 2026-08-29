from datetime import datetime
from io import BytesIO

import os

import boto3
import pandas as pd
import vertica_python

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "final-project")
STG_SCHEMA = os.getenv("VERTICA_STG_SCHEMA", "STAGING")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 10, 1),
    "end_date": datetime(2022, 10, 31),
    "retries": 1,
}


dag = DAG(
    dag_id="s3_to_vertica_stg",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
)


def get_s3_client():
    conn = BaseHook.get_connection("s3_conn")
    extra = conn.extra_dejson

    return boto3.client(
        service_name="s3",
        endpoint_url=extra.get("endpoint_url"),
        aws_access_key_id=extra.get("aws_access_key_id"),
        aws_secret_access_key=extra.get("aws_secret_access_key"),
    )


def get_vertica_conn_info():
    conn = BaseHook.get_connection("vertica_conn")

    return {
        "host": conn.host,
        "port": conn.port,
        "user": conn.login,
        "password": conn.password,
        "database": conn.schema,
        "autocommit": True,
    }


def read_csv_from_s3(s3_client, file_name):
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_name)
    return pd.read_csv(BytesIO(obj["Body"].read()))


def load_data(**context):
    execution_date = context["logical_date"]
    exec_date = execution_date.strftime("%Y-%m-%d")

    s3_client = get_s3_client()

    currencies_df = read_csv_from_s3(s3_client, "currencies_history.csv")
    currencies_df = currencies_df[currencies_df["date_update"] == exec_date]

    transactions_df_list = []

    for i in range(1, 11):
        file_name = f"transactions_batch_{i}.csv"
        df = read_csv_from_s3(s3_client, file_name)

        df["transaction_dt"] = pd.to_datetime(df["transaction_dt"])
        df = df[df["transaction_dt"].dt.strftime("%Y-%m-%d") == exec_date]

        if not df.empty:
            transactions_df_list.append(df)

    if transactions_df_list:
        transactions_df = pd.concat(transactions_df_list, ignore_index=True)
        transactions_df = transactions_df.sort_values("transaction_dt")
        transactions_df = transactions_df.drop_duplicates(
            subset=["operation_id", "transaction_dt"],
            keep="last",
        )
    else:
        transactions_df = pd.DataFrame(
            columns=[
                "operation_id",
                "account_number_from",
                "account_number_to",
                "currency_code",
                "country",
                "status",
                "transaction_type",
                "amount",
                "transaction_dt",
            ]
        )

    currencies_rows = [
        (
            int(row.currency_code),
            int(row.currency_code_with),
            row.date_update,
            float(row.currency_with_div),
        )
        for row in currencies_df.itertuples(index=False)
    ]

    transactions_rows = [
        (
            row.operation_id,
            int(row.account_number_from),
            int(row.account_number_to),
            int(row.currency_code),
            row.country,
            row.status,
            row.transaction_type,
            int(row.amount),
            row.transaction_dt.to_pydatetime(),
        )
        for row in transactions_df.itertuples(index=False)
    ]

    with vertica_python.connect(**get_vertica_conn_info()) as conn:
        cur = conn.cursor()

        cur.execute(f"""
            DELETE FROM {STG_SCHEMA}.currencies
            WHERE date_update = '{exec_date}'
        """)

        cur.execute(f"""
            DELETE FROM {STG_SCHEMA}.transactions
            WHERE transaction_dt::date = '{exec_date}'
        """)

        if currencies_rows:
            cur.executemany(
                f"""
                INSERT INTO {STG_SCHEMA}.currencies
                (
                    currency_code,
                    currency_code_with,
                    date_update,
                    currency_with_div
                )
                VALUES (%s, %s, %s, %s)
                """,
                currencies_rows,
            )

        if transactions_rows:
            cur.executemany(
                f"""
                INSERT INTO {STG_SCHEMA}.transactions
                (
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                transactions_rows,
            )

        print(f"Loaded date: {exec_date}")
        print(f"Currencies rows: {len(currencies_rows)}")
        print(f"Transactions rows: {len(transactions_rows)}")


load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)