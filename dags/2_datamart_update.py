from datetime import datetime, timedelta

import os

import vertica_python

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


STG_SCHEMA = os.getenv("VERTICA_STG_SCHEMA", "STAGING")
DWH_SCHEMA = os.getenv("VERTICA_DWH_SCHEMA", "DWH")

USD_CODE = 420


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 10, 2),
    "end_date": datetime(2022, 11, 1),
    "retries": 1,
}


dag = DAG(
    dag_id="stg_to_global_metrics",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
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


def load_global_metrics(**context):
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

    with vertica_python.connect(**get_vertica_conn_info()) as conn:
        cur = conn.cursor()

        cur.execute(f"""
            DELETE FROM {DWH_SCHEMA}.global_metrics
            WHERE date_update = DATE '{target_date}'
        """)

        cur.execute(f"""
            INSERT INTO {DWH_SCHEMA}.global_metrics (
                date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions
            )
            SELECT
                DATE '{target_date}' AS date_update,
                t.currency_code AS currency_from,

                SUM(
                    CASE
                        WHEN t.currency_code = {USD_CODE}
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

            FROM {STG_SCHEMA}.transactions t

            LEFT JOIN {STG_SCHEMA}.currencies c
                ON c.currency_code = t.currency_code
               AND c.currency_code_with = {USD_CODE}
               AND c.date_update = DATE '{target_date}'

            WHERE t.transaction_dt::date = DATE '{target_date}'
              AND t.account_number_from >= 0
              AND t.account_number_to >= 0
              AND t.status = 'done'

            GROUP BY
                t.currency_code
        """)

        print(f"Loaded global_metrics for date: {target_date}")


load_task = PythonOperator(
    task_id="load_global_metrics",
    python_callable=load_global_metrics,
    dag=dag,
)