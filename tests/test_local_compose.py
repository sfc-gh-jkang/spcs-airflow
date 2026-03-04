"""Local integration tests using Docker Compose.

Spins up the local Airflow stack (postgres + api-server + scheduler),
exercises the full DAG lifecycle via the Airflow 3.x REST API, and
tears everything down.

Requirements:
    - Docker Desktop running
    - Port 8080 free (Airflow api-server)
    - Port 5432 free (PostgreSQL)

Run with:
    pytest tests/test_local_compose.py -m local -v
"""

from __future__ import annotations

import datetime
import subprocess
import time
from pathlib import Path

import pytest
import requests

PROJECT_DIR = Path(__file__).resolve().parent.parent
COMPOSE_FILE = PROJECT_DIR / "docker-compose.yaml"
AIRFLOW_URL = "http://localhost:8080"

TEST_DAG_ID = "example_taskflow"

# Timeouts
STARTUP_TIMEOUT = 180  # max seconds for docker compose up + health
HEALTH_POLL_INTERVAL = 5
DAG_PARSE_TIMEOUT = 90  # max seconds to wait for DAG to appear
RUN_POLL_TIMEOUT = 120  # max seconds for DAG run to complete
RUN_POLL_INTERVAL = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_jwt_token() -> str:
    """Authenticate with Airflow 3.x and return a JWT token."""
    resp = requests.post(
        f"{AIRFLOW_URL}/auth/token",
        json={"username": "admin", "password": "admin"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def api_get(path: str, token: str) -> requests.Response:
    """GET request to the Airflow REST API v2."""
    return requests.get(
        f"{AIRFLOW_URL}/api/v2{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )


def api_post(path: str, token: str, data: dict | None = None) -> requests.Response:
    """POST request to the Airflow REST API v2."""
    return requests.post(
        f"{AIRFLOW_URL}/api/v2{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=data or {},
        timeout=15,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def local_airflow():
    """Start the Docker Compose stack, wait for health, yield, then tear down.

    Uses --wait to block until healthchecks pass. Falls back to polling
    /api/v2/monitor/health if --wait is not supported.
    """
    # Start the stack
    start_result = subprocess.run(
        [
            "docker", "compose",
            "-f", str(COMPOSE_FILE),
            "up", "-d", "--build", "--wait",
        ],
        capture_output=True,
        text=True,
        timeout=STARTUP_TIMEOUT,
        cwd=str(PROJECT_DIR),
    )
    if start_result.returncode != 0:
        # Try without --wait for older docker compose versions
        start_result = subprocess.run(
            [
                "docker", "compose",
                "-f", str(COMPOSE_FILE),
                "up", "-d", "--build",
            ],
            capture_output=True,
            text=True,
            timeout=STARTUP_TIMEOUT,
            cwd=str(PROJECT_DIR),
        )
        if start_result.returncode != 0:
            pytest.fail(
                f"docker compose up failed (rc={start_result.returncode}):\n"
                f"STDOUT: {start_result.stdout}\nSTDERR: {start_result.stderr}"
            )

    # Poll /api/v2/monitor/health until the api-server is responsive
    # (Airflow 3.x moved /health to /api/v2/monitor/health)
    health_url = f"{AIRFLOW_URL}/api/v2/monitor/health"
    deadline = time.time() + STARTUP_TIMEOUT
    last_error = None
    while time.time() < deadline:
        try:
            resp = requests.get(health_url, timeout=5)
            if resp.status_code == 200:
                break
        except requests.ConnectionError as e:
            last_error = e
        time.sleep(HEALTH_POLL_INTERVAL)
    else:
        # Dump logs for debugging
        logs = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "logs", "--tail=50"],
            capture_output=True, text=True, cwd=str(PROJECT_DIR),
        )
        pytest.fail(
            f"Airflow api-server not healthy after {STARTUP_TIMEOUT}s.\n"
            f"Last error: {last_error}\n"
            f"Container logs:\n{logs.stdout[-3000:]}"
        )

    yield AIRFLOW_URL

    # Teardown: stop and remove containers + volumes
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=str(PROJECT_DIR),
    )


@pytest.fixture(scope="module")
def jwt_token(local_airflow):
    """Get a JWT token for the local Airflow instance."""
    return get_jwt_token()


@pytest.fixture(scope="module")
def dag_run(local_airflow, jwt_token):
    """Wait for the test DAG to be parsed, trigger it, poll until complete.

    Returns the final DAG run dict.
    """
    # Wait for the DAG to appear in the API
    deadline = time.time() + DAG_PARSE_TIMEOUT
    dag_found = False
    dag_ids = []
    while time.time() < deadline:
        resp = api_get("/dags", jwt_token)
        if resp.status_code == 200:
            dags = resp.json().get("dags", [])
            dag_ids = [d["dag_id"] for d in dags]
            if TEST_DAG_ID in dag_ids:
                dag_found = True
                break
        time.sleep(3)

    if not dag_found:
        pytest.fail(
            f"DAG '{TEST_DAG_ID}' not found after {DAG_PARSE_TIMEOUT}s. "
            f"Available DAGs: {dag_ids}"
        )

    # Trigger the DAG (Airflow 3.x requires logical_date)
    logical_date = datetime.datetime.now(datetime.timezone.utc).isoformat()
    trigger_resp = api_post(
        f"/dags/{TEST_DAG_ID}/dagRuns",
        jwt_token,
        data={"logical_date": logical_date},
    )
    assert trigger_resp.status_code == 200, (
        f"Failed to trigger DAG: {trigger_resp.status_code} {trigger_resp.text}"
    )
    run_data = trigger_resp.json()
    run_id = run_data["dag_run_id"]

    # Poll until the run completes
    deadline = time.time() + RUN_POLL_TIMEOUT
    state = "unknown"
    while time.time() < deadline:
        status_resp = api_get(
            f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}", jwt_token
        )
        if status_resp.status_code == 200:
            run_info = status_resp.json()
            state = run_info.get("state")
            if state in ("success", "failed"):
                return run_info
        time.sleep(RUN_POLL_INTERVAL)

    pytest.fail(
        f"DAG run '{run_id}' did not complete after {RUN_POLL_TIMEOUT}s. "
        f"Last state: {state}"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.local
class TestLocalAirflowStack:
    """Local integration tests: Docker Compose -> REST API -> DAG execution."""

    def test_health_endpoint(self, local_airflow):
        """Airflow /api/v2/monitor/health returns 200 (api-server + scheduler + DB are up)."""
        resp = requests.get(f"{local_airflow}/api/v2/monitor/health", timeout=10)
        assert resp.status_code == 200

    def test_dags_are_loaded(self, local_airflow, jwt_token):
        """The scheduler has parsed our DAGs from the bind-mounted dags/ directory."""
        # Poll briefly in case parsing is still in progress
        deadline = time.time() + DAG_PARSE_TIMEOUT
        dag_ids = []
        while time.time() < deadline:
            resp = api_get("/dags", jwt_token)
            if resp.status_code == 200:
                dags = resp.json().get("dags", [])
                dag_ids = [d["dag_id"] for d in dags]
                if TEST_DAG_ID in dag_ids:
                    break
            time.sleep(3)

        assert TEST_DAG_ID in dag_ids, (
            f"DAG '{TEST_DAG_ID}' not found. Available: {dag_ids}"
        )

    def test_dag_run_succeeds(self, dag_run):
        """Triggered DAG run completes with state=success."""
        assert dag_run["state"] == "success", (
            f"DAG run state is '{dag_run['state']}', expected 'success'"
        )

    def test_all_task_instances_succeeded(self, dag_run, jwt_token):
        """All 3 task instances (extract, transform, load) completed successfully."""
        run_id = dag_run["dag_run_id"]
        resp = api_get(
            f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances",
            jwt_token,
        )
        assert resp.status_code == 200

        tasks = resp.json().get("task_instances", [])
        task_states = {t["task_id"]: t["state"] for t in tasks}

        expected_tasks = ["extract", "transform", "load"]
        for task_id in expected_tasks:
            assert task_id in task_states, (
                f"Task '{task_id}' not found. Tasks: {list(task_states.keys())}"
            )
            assert task_states[task_id] == "success", (
                f"Task '{task_id}' state is '{task_states[task_id]}', expected 'success'"
            )

    def test_xcom_data_flowed(self, dag_run, jwt_token):
        """XCom data passed between tasks (extract -> transform -> load)."""
        run_id = dag_run["dag_run_id"]

        # Check the extract task's XCom -- should have the raw data dict
        resp = api_get(
            f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/extract/xcomEntries",
            jwt_token,
        )
        assert resp.status_code == 200
        xcom_entries = resp.json().get("xcom_entries", [])
        assert len(xcom_entries) > 0, "extract task should have XCom output"
