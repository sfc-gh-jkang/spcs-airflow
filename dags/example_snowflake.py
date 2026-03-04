"""Example Snowflake queries — works on SPCS (OAuth) and locally (env vars).

Demonstrates:
- Auto-detecting SPCS vs local environment
- Airflow 3.x TaskFlow API with @task decorator
- Shared connection helper from utils.snowflake_conn
"""

import logging

import pendulum
from airflow.sdk import DAG, task

from utils.snowflake_conn import get_snowflake_connection

logger = logging.getLogger(__name__)


with DAG(
    dag_id="example_snowflake",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["example", "snowflake", "spcs"],
    doc_md="""
    ## Example Snowflake DAG
    Runs simple queries against Snowflake.
    Auto-detects SPCS OAuth vs local env var credentials.
    """,
):

    @task
    def query_current_timestamp():
        """Query current timestamp and account info."""
        conn = get_snowflake_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_TIMESTAMP() AS ts, CURRENT_ACCOUNT() AS account")
            row = cur.fetchone()
            logger.info("Timestamp: %s, Account: %s", row[0], row[1])
            return {"timestamp": str(row[0]), "account": row[1]}
        finally:
            cur.close()
            conn.close()

    @task
    def query_warehouse_info(account_info: dict):
        """List available warehouses."""
        conn = get_snowflake_connection()
        try:
            cur = conn.cursor()
            cur.execute("SHOW WAREHOUSES")
            warehouses = cur.fetchall()
            logger.info("Account %s has %d warehouse(s)", account_info["account"], len(warehouses))
            for wh in warehouses:
                logger.info("  - %s", wh[0])
            return len(warehouses)
        finally:
            cur.close()
            conn.close()

    info = query_current_timestamp()
    query_warehouse_info(info)
