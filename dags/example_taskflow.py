"""Example TaskFlow DAG using Airflow 3.x SDK.

Demonstrates:
- TaskFlow API with @task decorator (Airflow 3.x style)
- Task dependencies via Python function calls
- XCom passing between tasks (JSONB in Airflow 3.x)
"""

import pendulum
from airflow.sdk import DAG, task


with DAG(
    dag_id="example_taskflow",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["example", "taskflow", "spcs"],
    doc_md="""
    ## Example TaskFlow DAG
    Demonstrates Airflow 3.x TaskFlow SDK on SPCS.
    Runs three tasks: extract → transform → load.
    """,
):

    @task
    def extract():
        """Simulate data extraction."""
        return {"records": [1, 2, 3, 4, 5], "source": "spcs_demo"}

    @task
    def transform(data: dict):
        """Transform extracted data."""
        records = data["records"]
        return {
            "count": len(records),
            "total": sum(records),
            "source": data["source"],
        }

    @task
    def load(summary: dict):
        """Load transformed data (log summary)."""
        print(f"Loaded {summary['count']} records from {summary['source']}")
        print(f"Total value: {summary['total']}")
        return summary

    raw_data = extract()
    transformed = transform(raw_data)
    load(transformed)
