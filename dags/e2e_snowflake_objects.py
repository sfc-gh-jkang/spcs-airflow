"""E2E test DAG — creates real Snowflake objects.

Purpose-built for end-to-end testing of the full pipeline:
  local dev → upload to stage → dag-processor parses → worker executes → Snowflake objects created

Tasks:
  create_table → insert_data → verify_data

The table is intentionally NOT dropped — the E2E test suite verifies its
contents via snow sql, then cleans up afterward.

Works on both SPCS (OAuth token) and locally (env var credentials).
Uses the shared connection helper from utils.snowflake_conn.

Schedule: manual trigger only (schedule=None).
"""

import logging

import pendulum
from airflow.sdk import DAG, task

from utils.snowflake_conn import run_sql

logger = logging.getLogger(__name__)

SNOWFLAKE_DB = "AIRFLOW_DB"
SNOWFLAKE_SCHEMA = "AIRFLOW_SCHEMA"
SNOWFLAKE_WAREHOUSE = "AIRFLOW_SETUP_WH"
TABLE_NAME = "E2E_TEST_RESULTS"
QUALIFIED_TABLE = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}"


def _run_sql(sql: str, fetch: bool = False):
    """Execute SQL with default database/schema/warehouse."""
    return run_sql(
        sql,
        fetch=fetch,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )


with DAG(
    dag_id="e2e_snowflake_objects",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["e2e", "snowflake", "spcs", "test"],
    doc_md="""
    ## E2E Snowflake Objects DAG
    Creates a table, inserts test data, and verifies row counts.
    Used by the E2E test suite to prove the full local → SPCS → Snowflake loop.
    The table is left for the test to inspect; cleanup is handled externally.
    """,
):

    @task
    def create_table():
        """Create the e2e_test_results table (idempotent)."""
        _run_sql(f"""
            CREATE OR REPLACE TABLE {QUALIFIED_TABLE} (
                test_id     VARCHAR(50),
                created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                value       INTEGER
            )
        """)
        logger.info("Created table %s", QUALIFIED_TABLE)
        return TABLE_NAME

    @task
    def insert_data(table_name: str):
        """Insert known test rows."""
        _run_sql(f"""
            INSERT INTO {QUALIFIED_TABLE} (test_id, value)
            VALUES
                ('row_1', 100),
                ('row_2', 200),
                ('row_3', 300)
        """)
        rows = _run_sql(
            f"SELECT COUNT(*) FROM {QUALIFIED_TABLE}", fetch=True
        )
        count = rows[0][0]
        logger.info("Inserted rows into %s, count=%d", QUALIFIED_TABLE, count)
        return count

    @task
    def verify_data(row_count: int):
        """Validate expected row count and values."""
        if row_count != 3:
            raise ValueError(
                f"Expected 3 rows in {QUALIFIED_TABLE}, got {row_count}"
            )

        rows = _run_sql(
            f"SELECT SUM(value) FROM {QUALIFIED_TABLE}", fetch=True
        )
        total = rows[0][0]
        if total != 600:
            raise ValueError(
                f"Expected SUM(value)=600 in {QUALIFIED_TABLE}, got {total}"
            )
        logger.info(
            "Verification passed: %d rows, SUM(value)=%d", row_count, total
        )
        return {"table": TABLE_NAME, "rows": row_count, "total": total}

    t1 = create_table()
    t2 = insert_data(t1)
    verify_data(t2)
