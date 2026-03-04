"""End-to-end tests against a live SPCS Airflow cluster.

These tests upload DAGs to the AIRFLOW_DAGS stage, wait for the
dag-processor to parse them, trigger a DAG run, and verify it succeeds.

Requirements:
    - A running SPCS Airflow deployment (all 7 services READY)
    - snow CLI configured with an 'aws_spcs' connection
    - WORKER_POOL compute pool with spare capacity

Run with:
    pytest tests/test_e2e_spcs.py -m e2e -v
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parent.parent
DAGS_DIR = PROJECT_DIR / "dags"
SYNC_SCRIPT = PROJECT_DIR / "scripts" / "sync_dags.sh"

CONNECTION = "aws_spcs"
SF_DATABASE = "AIRFLOW_DB"
SF_SCHEMA = "AIRFLOW_SCHEMA"
SF_QUALIFIED = f"{SF_DATABASE}.{SF_SCHEMA}"
COMPUTE_POOL = "WORKER_POOL"
AIRFLOW_IMAGE = "/airflow_db/airflow_schema/airflow_repository/airflow:3.1.7"

# Timeouts
STAGE_SYNC_WAIT = 5  # seconds to wait after upload for stage sync
JOB_SERVICE_TIMEOUT = 600  # max seconds for EXECUTE JOB SERVICE

# DAG used for E2E testing (pure Python, no Snowflake connection needed)
TEST_DAG_ID = "example_taskflow"

# DAG that creates real Snowflake objects (tests full SPCS → Snowflake loop)
SF_OBJECTS_DAG_ID = "e2e_snowflake_objects"
SF_OBJECTS_TABLE = "E2E_TEST_RESULTS"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def run_snow_sql(query: str, *, timeout: int = 120, fmt: str = "json") -> str:
    """Execute a SQL query via snow CLI and return stdout.

    Uses --format json by default for reliable parsing of multi-line values.
    """
    full_query = (
        f"USE DATABASE {SF_DATABASE}; "
        f"USE SCHEMA {SF_SCHEMA}; "
        f"{query}"
    )
    cmd = ["snow", "sql", "--connection", CONNECTION, "-q", full_query]
    if fmt:
        cmd.extend(["--format", fmt])
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"snow sql failed (rc={result.returncode}):\n"
            f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )
    return result.stdout


def run_airflow_cli(
    command: str,
    *,
    job_name: str | None = None,
    timeout: int = 300,
) -> str:
    """Run an Airflow CLI command inside an EXECUTE JOB SERVICE container.

    Returns the container logs (stdout from the Airflow CLI).
    The job service is cleaned up afterward.
    """
    if job_name is None:
        job_name = f"E2E_{uuid.uuid4().hex[:8].upper()}"

    spec = f"""
  spec:
    containers:
      - name: airflow-cli
        image: {AIRFLOW_IMAGE}
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "postgresql+psycopg2://airflow:{{{{secret.airflow_postgres_pwd.secret_string}}}}@af-postgres:5432/airflow"
          AIRFLOW__CORE__EXECUTOR: LocalExecutor
          AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"
        command:
          - /bin/bash
          - -c
          - "{command}"
        secrets:
          - snowflakeSecret:
              objectName: airflow_postgres_pwd
        volumeMounts:
          - name: dags-volume
            mountPath: /opt/airflow/dags
    volumes:
      - name: dags-volume
        source: stage
        stageConfig:
          name: "@{SF_QUALIFIED}.AIRFLOW_DAGS"
        uid: 50000
        gid: 0
"""
    execute_query = (
        f"EXECUTE JOB SERVICE IN COMPUTE POOL {COMPUTE_POOL} "
        f"FROM SPECIFICATION $${spec}$$ "
        f"NAME = '{job_name}';"
    )

    try:
        run_snow_sql(execute_query, timeout=timeout, fmt="")

        # Fetch logs — use JSON format so the log text isn't mangled by
        # table-format column wrapping.  snow sql --format json returns
        # an array-of-arrays (one per SQL statement).  The last array
        # contains the CALL result: [{"SYSTEM$GET_SERVICE_LOGS": "…"}].
        logs_query = (
            f"CALL SYSTEM$GET_SERVICE_LOGS("
            f"'{SF_QUALIFIED}.{job_name}', 0, 'airflow-cli', 500);"
        )
        raw = run_snow_sql(logs_query, timeout=30, fmt="json")
        try:
            result_sets = json.loads(raw)
            # Last result set → first row → the log column value
            log_text = result_sets[-1][0].get(
                "SYSTEM$GET_SERVICE_LOGS", ""
            )
        except (json.JSONDecodeError, IndexError, KeyError):
            log_text = raw  # fall back to raw output
        return log_text
    finally:
        # Always clean up the job service
        try:
            run_snow_sql(
                f"DROP SERVICE IF EXISTS {SF_QUALIFIED}.{job_name};",
                timeout=30,
                fmt="",
            )
        except Exception:
            pass  # best-effort cleanup


def parse_airflow_json_from_logs(logs: str) -> list[dict]:
    """Extract the JSON array from Airflow CLI -o json output in service logs.

    The logs contain service log noise (alembic setup lines, etc.) followed
    by the JSON output. We find the first '[' and parse from there.
    """
    # The JSON array starts with '[' — find it in the raw output
    bracket_idx = logs.find("[{")
    if bracket_idx == -1:
        # Try to find just '[' for empty arrays
        bracket_idx = logs.find("[]")
        if bracket_idx != -1:
            return []
        return []

    # Find the matching closing bracket
    depth = 0
    start = bracket_idx
    for i in range(start, len(logs)):
        if logs[i] == "[":
            depth += 1
        elif logs[i] == "]":
            depth -= 1
            if depth == 0:
                json_str = logs[start : i + 1]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    return []
    return []


def query_snowflake_json(query: str) -> list[dict]:
    """Run a SQL query via snow sql --format json and return parsed rows.

    Returns the result set from the last SQL statement (after USE DATABASE/SCHEMA).
    """
    raw = run_snow_sql(query, fmt="json")
    try:
        result_sets = json.loads(raw)
        return result_sets[-1] if result_sets else []
    except (json.JSONDecodeError, IndexError):
        return []


def list_stage_files() -> list[str]:
    """Return list of file paths on the AIRFLOW_DAGS stage.

    Uses --format json for reliable parsing (table format wraps long names).
    The snow CLI returns a JSON array of arrays — one per SQL statement.
    The LIST results are in the last array element.
    """
    output = run_snow_sql(f"LIST @{SF_QUALIFIED}.AIRFLOW_DAGS;")
    try:
        result_sets = json.loads(output)
    except json.JSONDecodeError:
        return []

    # The last result set contains the LIST output
    if not result_sets:
        return []
    list_results = result_sets[-1]
    return [row["name"] for row in list_results if "name" in row]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def upload_dags():
    """Upload all DAGs (including utils/) to the SPCS stage via sync_dags.sh.

    Runs once per test module. Returns the script output.
    """
    result = subprocess.run(
        ["bash", str(SYNC_SCRIPT), "--connection", CONNECTION],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=str(PROJECT_DIR),
    )
    if result.returncode != 0:
        pytest.fail(
            f"sync_dags.sh failed (rc={result.returncode}):\n"
            f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )
    # Give stage volumes a moment to sync
    time.sleep(STAGE_SYNC_WAIT)
    return result.stdout


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestStageUpload:
    """Phase 1-2: Upload DAGs and verify stage contents."""

    def test_sync_dags_succeeds(self, upload_dags):
        """sync_dags.sh completes without error."""
        assert "Done" in upload_dags

    def test_stage_has_top_level_dags(self, upload_dags):
        """Stage contains the expected top-level DAG files."""
        files = list_stage_files()
        filenames = [f.split("/")[-1] for f in files]
        assert "example_taskflow.py" in filenames
        assert "snowflake_etl_pipeline.py" in filenames

    def test_stage_has_utils_subdir(self, upload_dags):
        """Stage contains the utils/ subdirectory with __init__.py and snowflake_conn.py."""
        files = list_stage_files()
        # Look for utils/ paths
        utils_files = [f for f in files if "/utils/" in f]
        utils_names = [f.split("/")[-1] for f in utils_files]
        assert "__init__.py" in utils_names, (
            f"utils/__init__.py not found on stage. Files: {files}"
        )
        assert "snowflake_conn.py" in utils_names, (
            f"utils/snowflake_conn.py not found on stage. Files: {files}"
        )


@pytest.mark.e2e
class TestDagParsing:
    """Phase 3: Verify the dag-processor has parsed our test DAG."""

    def test_dag_is_parsed(self, upload_dags):
        """The dag-processor recognizes example_taskflow after upload.

        Runs 'airflow dags list -o json' in a single container.
        The live dag-processor continuously parses DAGs, so this should
        find the DAG without polling.
        """
        logs = run_airflow_cli(
            "airflow dags list -o json 2>&1",
            timeout=300,
        )
        dags = parse_airflow_json_from_logs(logs)
        dag_ids = [d.get("dag_id") for d in dags]

        assert TEST_DAG_ID in dag_ids, (
            f"DAG '{TEST_DAG_ID}' not found. Parsed DAGs: {dag_ids}"
        )


@pytest.mark.e2e
class TestDagExecution:
    """Phase 4-5: Trigger a DAG run and verify it succeeds.

    Runs trigger + poll inside a SINGLE EXECUTE JOB SERVICE container
    to avoid the ~60-90s cold-start penalty per container.
    """

    def test_trigger_and_complete(self, upload_dags):
        """Trigger example_taskflow, poll for success inside one container."""
        # Single bash script: trigger, then poll every 5s for up to 120s.
        # All-in-one container avoids ~90s cold-start per poll iteration.
        # IMPORTANT: no double quotes allowed — the script is embedded in
        # a YAML string delimited by double quotes.
        script = (
            f"airflow dags trigger {TEST_DAG_ID} 2>&1; "
            "echo '=== POLLING ==='; "
            "for i in $(seq 1 24); do "
            "  sleep 5; "
            f"  STATE=$(airflow dags list-runs {TEST_DAG_ID} --state success -o plain 2>/dev/null "
            "    | grep -c manual__ || true); "
            "  echo poll=$i matches=$STATE; "
            "  if [ $STATE -gt 0 ]; then echo E2E_RESULT=SUCCESS; exit 0; fi; "
            "done; "
            "echo E2E_RESULT=TIMEOUT; exit 1"
        )

        logs = run_airflow_cli(script, timeout=600)

        assert "E2E_RESULT=SUCCESS" in logs, (
            f"DAG run did not succeed. Container logs:\n{logs[-2000:]}"
        )


@pytest.mark.e2e
class TestSnowflakeObjects:
    """Phase 6: Trigger a DAG that creates real Snowflake objects, then verify them.

    This is the full-loop test: local DAG → uploaded to stage → dag-processor
    parses it → worker executes tasks → Snowflake table is created with data.

    After verification, the test cleans up the table.
    """

    def test_trigger_creates_snowflake_objects(self, upload_dags):
        """Trigger e2e_snowflake_objects, verify it creates a real Snowflake table.

        Steps:
        1. Drop the table if it exists (clean slate)
        2. Trigger the DAG and poll for success (single container)
        3. Query Snowflake to verify the table exists with expected data
        4. Clean up the table
        """
        # 1. Clean slate — drop the table if left over from a previous run
        try:
            run_snow_sql(
                f"DROP TABLE IF EXISTS {SF_QUALIFIED}.{SF_OBJECTS_TABLE};",
                timeout=30,
                fmt="",
            )
        except Exception:
            pass  # best-effort

        # 2. Trigger + poll in a single container (same pattern as TestDagExecution)
        script = (
            f"airflow dags trigger {SF_OBJECTS_DAG_ID} 2>&1; "
            "echo '=== POLLING ==='; "
            "for i in $(seq 1 36); do "
            "  sleep 5; "
            f"  STATE=$(airflow dags list-runs {SF_OBJECTS_DAG_ID} --state success -o plain 2>/dev/null "
            "    | grep -c manual__ || true); "
            "  echo poll=$i matches=$STATE; "
            "  if [ $STATE -gt 0 ]; then echo E2E_RESULT=SUCCESS; exit 0; fi; "
            "done; "
            "echo E2E_RESULT=TIMEOUT; exit 1"
        )

        logs = run_airflow_cli(script, timeout=600)

        assert "E2E_RESULT=SUCCESS" in logs, (
            f"DAG run did not succeed. Container logs:\n{logs[-2000:]}"
        )

        # 3. Verify the Snowflake table exists with expected data
        try:
            rows = query_snowflake_json(
                f"SELECT COUNT(*) AS cnt FROM {SF_QUALIFIED}.{SF_OBJECTS_TABLE};"
            )
            assert len(rows) > 0, (
                f"Table {SF_OBJECTS_TABLE} query returned no rows"
            )
            count = rows[0].get("CNT", rows[0].get("cnt", 0))
            assert count == 3, (
                f"Expected 3 rows in {SF_OBJECTS_TABLE}, got {count}"
            )

            # Verify the sum matches expected value
            sum_rows = query_snowflake_json(
                f"SELECT SUM(VALUE) AS total FROM {SF_QUALIFIED}.{SF_OBJECTS_TABLE};"
            )
            total = sum_rows[0].get("TOTAL", sum_rows[0].get("total", 0))
            assert total == 600, (
                f"Expected SUM(value)=600 in {SF_OBJECTS_TABLE}, got {total}"
            )
        finally:
            # 4. Always clean up the table
            try:
                run_snow_sql(
                    f"DROP TABLE IF EXISTS {SF_QUALIFIED}.{SF_OBJECTS_TABLE};",
                    timeout=30,
                    fmt="",
                )
            except Exception:
                pass  # best-effort cleanup
