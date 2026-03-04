"""Snowflake ETL Pipeline — ingest, transform, validate.

Demonstrates a real end-to-end data pipeline on Snowflake via Airflow 3.x:
  create_raw_table → ingest_raw_data → transform_to_summary → validate_results → cleanup_raw

Works on both SPCS (OAuth token) and locally (env var credentials).
Uses the shared connection helper from utils.snowflake_conn.

Schedule: manual trigger only (schedule=None).
Self-contained: creates and cleans up its own tables.
"""

import logging

import pendulum
from airflow.sdk import DAG, task

from utils.snowflake_conn import get_snowflake_connection, run_sql

logger = logging.getLogger(__name__)

SNOWFLAKE_DB = "AIRFLOW_DB"
SNOWFLAKE_SCHEMA = "AIRFLOW_SCHEMA"
SNOWFLAKE_WAREHOUSE = "AIRFLOW_SETUP_WH"


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
    dag_id="snowflake_etl_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["etl", "snowflake", "spcs", "demo"],
    doc_md="""
    ## Snowflake ETL Pipeline
    End-to-end demo: ingests sample sales data into a raw table,
    transforms it into an aggregated summary, validates the output,
    and cleans up.

    Works on SPCS (OAuth) and locally (env var credentials).
    Trigger manually to see it run.
    """,
):

    @task
    def create_raw_table():
        """Create the raw_sales staging table."""
        _run_sql("""
            CREATE OR REPLACE TABLE raw_sales (
                sale_id     INTEGER,
                product     VARCHAR(50),
                quantity    INTEGER,
                unit_price  DECIMAL(10, 2),
                sale_date   DATE
            )
        """)
        return "raw_sales table created"

    @task
    def ingest_raw_data(status: str):
        """Insert sample sales data into raw_sales."""
        _run_sql("""
            INSERT INTO raw_sales (sale_id, product, quantity, unit_price, sale_date)
            VALUES
                (1,  'Laptop',      2,  999.99, '2026-01-15'),
                (2,  'Mouse',      10,   29.99, '2026-01-16'),
                (3,  'Keyboard',    5,   79.99, '2026-01-16'),
                (4,  'Monitor',     3,  349.99, '2026-01-17'),
                (5,  'Laptop',      1,  999.99, '2026-01-18'),
                (6,  'Mouse',      15,   29.99, '2026-01-18'),
                (7,  'Headphones',  8,   59.99, '2026-01-19'),
                (8,  'Keyboard',    3,   79.99, '2026-01-20'),
                (9,  'Monitor',     1,  349.99, '2026-01-21'),
                (10, 'Headphones', 12,   59.99, '2026-01-22')
        """)
        rows = _run_sql("SELECT COUNT(*) FROM raw_sales", fetch=True)
        count = rows[0][0]
        logger.info("Ingested %d rows into raw_sales", count)
        return count

    @task
    def transform_to_summary(row_count: int):
        """Aggregate raw sales into a product summary table."""
        _run_sql("""
            CREATE OR REPLACE TABLE sales_summary AS
            SELECT
                product,
                COUNT(*)                       AS num_transactions,
                SUM(quantity)                  AS total_quantity,
                SUM(quantity * unit_price)     AS total_revenue,
                ROUND(AVG(unit_price), 2)      AS avg_unit_price
            FROM raw_sales
            GROUP BY product
            ORDER BY total_revenue DESC
        """)
        results = _run_sql("""
            SELECT product, total_quantity, total_revenue
            FROM sales_summary
            ORDER BY total_revenue DESC
        """, fetch=True)
        for product, qty, revenue in results:
            logger.info("  %s: %d units, $%,.2f revenue", product, qty, revenue)
        return len(results)

    @task
    def validate_results(product_count: int):
        """Validate the summary has expected data."""
        rows = _run_sql("""
            SELECT
                COUNT(*)           AS product_count,
                SUM(total_revenue) AS grand_total
            FROM sales_summary
        """, fetch=True)
        count, total = rows[0]
        logger.info("Products: %d, Grand total revenue: $%,.2f", count, total)
        if count != 5:
            raise ValueError(f"Expected 5 products, got {count}")
        if total <= 0:
            raise ValueError(f"Revenue should be positive, got {total}")
        return {"products": count, "grand_total": float(total)}

    @task
    def cleanup_raw(validation: dict):
        """Drop the raw staging table (summary table kept for inspection)."""
        _run_sql("DROP TABLE IF EXISTS raw_sales")
        logger.info("Cleaned up raw_sales. Summary table retained with %d products.", validation['products'])
        return "done"

    t1 = create_raw_table()
    t2 = ingest_raw_data(t1)
    t3 = transform_to_summary(t2)
    t4 = validate_results(t3)
    cleanup_raw(t4)
