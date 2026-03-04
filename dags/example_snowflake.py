"""Example Snowflake queries using SPCS OAuth — zero-config authentication.

Demonstrates:
- SPCS OAuth token at /snowflake/session/token
- Airflow 3.x TaskFlow API with @task decorator
- No Snowflake connection URI or credentials needed
"""

import logging
import os

import pendulum
from airflow.sdk import DAG, task

logger = logging.getLogger(__name__)


def get_snowflake_connection():
    """Create a Snowflake connection using the SPCS OAuth token."""
    import snowflake.connector

    host = os.getenv("SNOWFLAKE_HOST")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    if not host or not account:
        raise RuntimeError(
            "SNOWFLAKE_HOST and SNOWFLAKE_ACCOUNT must be set (provided automatically inside SPCS)"
        )

    with open("/snowflake/session/token", "r") as f:
        token = f.read()

    return snowflake.connector.connect(
        host=host,
        account=account,
        token=token,
        authenticator="oauth",
    )


with DAG(
    dag_id="example_snowflake",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["example", "snowflake", "spcs"],
    doc_md="""
    ## Example Snowflake DAG
    Runs simple queries against Snowflake using the SPCS OAuth token.
    No connection configuration required — works automatically inside SPCS.
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
