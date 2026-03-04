"""Tests for docker-compose.yaml configuration validation.

Catches env var drift between SPCS production specs and local docker-compose,
validates Airflow 3.x requirements, and checks structural correctness.

These tests are offline (no Docker needed) and run as part of the offline test suite.
"""

import os

import pytest
import yaml

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
COMPOSE_PATH = os.path.join(PROJECT_ROOT, "docker-compose.yaml")
SPECS_DIR = os.path.join(PROJECT_ROOT, "specs")

# SPCS Airflow specs (services that have Airflow env vars)
SPCS_AIRFLOW_SPECS = [
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_dag_processor.yaml",
    "af_triggerer.yaml",
    "af_workers.yaml",
]

# Env vars required in docker-compose shared config
COMPOSE_REQUIRED_ENV_VARS = [
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW__CORE__EXECUTOR",
    "AIRFLOW__CORE__DAGS_FOLDER",
    "AIRFLOW__CORE__EXECUTION_API_SERVER_URL",
    "AIRFLOW__API_AUTH__JWT_SECRET",
    "AIRFLOW__API__SECRET_KEY",
    "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION",
    "AIRFLOW__CORE__LOAD_EXAMPLES",
]

# Env vars intentionally different between SPCS (CeleryExecutor) and compose (LocalExecutor)
SPCS_ONLY_ENV_VARS = {
    # Celery vars — compose uses LocalExecutor, no Redis
    "AIRFLOW__CELERY__BROKER_URL",
    "AIRFLOW__CELERY__RESULT_BACKEND",
    # SPCS-specific logging config
    "AIRFLOW__LOGGING__BASE_LOG_FOLDER",
    "AIRFLOW__SCHEDULER__SYMLINK_LATEST_LOG",
    # Auth backend config (SPCS uses basic_auth, compose uses simple_auth_manager)
    "AIRFLOW__API__AUTH_BACKENDS",
}

# Required docker-compose services
REQUIRED_COMPOSE_SERVICES = [
    "postgres",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
    "airflow-dag-processor",
]


def _load_compose():
    """Load and return the parsed docker-compose.yaml."""
    if not os.path.isfile(COMPOSE_PATH):
        pytest.skip("docker-compose.yaml not found")
    with open(COMPOSE_PATH) as f:
        return yaml.safe_load(f)


def _get_compose_shared_env(compose_data):
    """Extract the shared environment dict from x-airflow-common."""
    common = compose_data.get("x-airflow-common", {})
    return common.get("environment", {})


def _get_compose_shared_env_keys(compose_data):
    """Extract env var keys from x-airflow-common."""
    env = _get_compose_shared_env(compose_data)
    if isinstance(env, dict):
        return set(env.keys())
    # Handle list-of-strings format: ["KEY=value", ...]
    if isinstance(env, list):
        return {item.split("=")[0] for item in env if "=" in str(item)}
    return set()


def _get_spcs_spec_env_keys(spec_path):
    """Extract env var keys from an SPCS spec YAML."""
    if not os.path.isfile(spec_path):
        return set()
    with open(spec_path) as f:
        data = yaml.safe_load(f)
    env_keys = set()
    for container in data.get("spec", {}).get("containers", []):
        env_dict = container.get("env", {})
        if isinstance(env_dict, dict):
            env_keys.update(env_dict.keys())
        elif isinstance(env_dict, list):
            for item in env_dict:
                if isinstance(item, dict) and "name" in item:
                    env_keys.add(item["name"])
                elif isinstance(item, str):
                    env_keys.add(item)
    return env_keys


def _get_common_spcs_env_keys():
    """Get env vars present in ALL SPCS Airflow specs (the universal set)."""
    all_key_sets = []
    for spec_file in SPCS_AIRFLOW_SPECS:
        path = os.path.join(SPECS_DIR, spec_file)
        keys = _get_spcs_spec_env_keys(path)
        if keys:
            all_key_sets.append(keys)
    if not all_key_sets:
        return set()
    return set.intersection(*all_key_sets)


class TestComposeRequiredEnvVars:
    """Docker-compose shared env must include all required Airflow config."""

    @pytest.mark.parametrize("env_var", COMPOSE_REQUIRED_ENV_VARS)
    def test_has_required_env_var(self, env_var):
        compose = _load_compose()
        env_keys = _get_compose_shared_env_keys(compose)
        assert env_var in env_keys, (
            f"docker-compose.yaml x-airflow-common: missing required env var {env_var}"
        )


class TestComposeSpcsEnvVarParity:
    """Core Airflow env vars in ALL SPCS specs must also be in docker-compose.

    This is the test that catches env var drift between production SPCS specs
    and the local docker-compose config. Both bugs in this session (missing
    EXECUTION_API_SERVER_URL and JWT_SECRET) would have been caught by this test.
    """

    def test_compose_has_all_common_spcs_airflow_vars(self):
        """Every AIRFLOW__* var present in ALL SPCS specs must be in compose
        (excluding intentional differences like Celery vars)."""
        common_spcs_keys = _get_common_spcs_env_keys()
        if not common_spcs_keys:
            pytest.skip("No SPCS specs found")

        compose = _load_compose()
        compose_keys = _get_compose_shared_env_keys(compose)

        # Filter to AIRFLOW__* vars only, exclude known SPCS-only vars
        airflow_spcs_keys = {
            k for k in common_spcs_keys
            if k.startswith("AIRFLOW__") and k not in SPCS_ONLY_ENV_VARS
        }

        missing = airflow_spcs_keys - compose_keys
        assert not missing, (
            f"docker-compose.yaml is missing {len(missing)} env var(s) that are "
            f"present in ALL SPCS specs: {sorted(missing)}. "
            f"If intentionally excluded, add to SPCS_ONLY_ENV_VARS in this test."
        )

    def test_spcs_only_exclusions_are_valid(self):
        """Every var in SPCS_ONLY_ENV_VARS must actually exist in SPCS specs.
        Prevents stale exclusions from hiding real drift."""
        common_spcs_keys = _get_common_spcs_env_keys()
        if not common_spcs_keys:
            pytest.skip("No SPCS specs found")

        # Check that exclusions reference real SPCS vars (from any spec, not just common)
        all_spcs_keys = set()
        for spec_file in SPCS_AIRFLOW_SPECS:
            path = os.path.join(SPECS_DIR, spec_file)
            all_spcs_keys.update(_get_spcs_spec_env_keys(path))

        stale = SPCS_ONLY_ENV_VARS - all_spcs_keys
        assert not stale, (
            f"SPCS_ONLY_ENV_VARS contains vars not found in any SPCS spec "
            f"(stale exclusions): {sorted(stale)}. Remove them."
        )


class TestComposeStructure:
    """Docker-compose must have correct services, healthchecks, and volumes."""

    @pytest.mark.parametrize("service_name", REQUIRED_COMPOSE_SERVICES)
    def test_required_service_exists(self, service_name):
        compose = _load_compose()
        services = compose.get("services", {})
        assert service_name in services, (
            f"docker-compose.yaml: missing required service '{service_name}'"
        )

    def test_dag_processor_exists(self):
        """Airflow 3.x requires a separate dag-processor service."""
        compose = _load_compose()
        services = compose.get("services", {})
        assert "airflow-dag-processor" in services, (
            "docker-compose.yaml: Airflow 3.x requires a dedicated "
            "airflow-dag-processor service (separate from scheduler)"
        )

    def test_dag_processor_runs_dag_processor_command(self):
        compose = _load_compose()
        service = compose["services"].get("airflow-dag-processor", {})
        command = str(service.get("command", ""))
        assert "dag-processor" in command, (
            "airflow-dag-processor service must run 'airflow dag-processor'"
        )

    def test_webserver_runs_api_server_command(self):
        """Airflow 3.x uses 'airflow api-server', not 'airflow webserver'."""
        compose = _load_compose()
        service = compose["services"].get("airflow-webserver", {})
        command = str(service.get("command", ""))
        assert "api-server" in command, (
            "airflow-webserver service must run 'airflow api-server' (Airflow 3.x)"
        )

    def test_webserver_exposes_port_8080(self):
        compose = _load_compose()
        service = compose["services"].get("airflow-webserver", {})
        ports = [str(p) for p in service.get("ports", [])]
        assert any("8080" in p for p in ports), (
            "airflow-webserver must expose port 8080"
        )

    def test_postgres_has_healthcheck(self):
        compose = _load_compose()
        service = compose["services"].get("postgres", {})
        assert "healthcheck" in service, (
            "postgres service must have a healthcheck"
        )

    def test_webserver_has_healthcheck(self):
        compose = _load_compose()
        service = compose["services"].get("airflow-webserver", {})
        assert "healthcheck" in service, (
            "airflow-webserver service must have a healthcheck"
        )

    def test_webserver_healthcheck_uses_v2_endpoint(self):
        compose = _load_compose()
        service = compose["services"].get("airflow-webserver", {})
        hc = service.get("healthcheck", {})
        test_cmd = str(hc.get("test", ""))
        assert "/api/v2/monitor/health" in test_cmd, (
            "airflow-webserver healthcheck must use /api/v2/monitor/health (Airflow 3.x)"
        )

    def test_dag_volume_mounted(self):
        """DAGs directory must be mounted into containers."""
        compose = _load_compose()
        common = compose.get("x-airflow-common", {})
        volumes = [str(v) for v in common.get("volumes", [])]
        assert any("dags" in v and "/opt/airflow/dags" in v for v in volumes), (
            "x-airflow-common must mount DAGs volume to /opt/airflow/dags"
        )

    def test_init_runs_db_migrate(self):
        compose = _load_compose()
        service = compose["services"].get("airflow-init", {})
        command = str(service.get("command", ""))
        assert "db migrate" in command, (
            "airflow-init must run 'airflow db migrate'"
        )


class TestComposeAirflow3Requirements:
    """Airflow 3.x-specific configuration requirements for docker-compose."""

    def test_execution_api_url_points_to_webserver(self):
        """EXECUTION_API_SERVER_URL must point to the webserver's /execution/ path."""
        compose = _load_compose()
        env = _get_compose_shared_env(compose)
        url = env.get("AIRFLOW__CORE__EXECUTION_API_SERVER_URL", "")
        assert "airflow-webserver" in url, (
            f"EXECUTION_API_SERVER_URL must reference the airflow-webserver service, "
            f"got: {url}"
        )
        assert url.rstrip("/").endswith("/execution"), (
            f"EXECUTION_API_SERVER_URL must end with /execution/, got: {url}"
        )
        assert url.startswith("http://"), (
            f"EXECUTION_API_SERVER_URL must use http:// (internal Docker network), "
            f"got: {url}"
        )

    def test_jwt_secret_is_set(self):
        """JWT secret must be explicitly set to prevent auto-generation divergence."""
        compose = _load_compose()
        env = _get_compose_shared_env(compose)
        jwt_secret = env.get("AIRFLOW__API_AUTH__JWT_SECRET", "")
        assert jwt_secret, (
            "AIRFLOW__API_AUTH__JWT_SECRET must be explicitly set in docker-compose. "
            "Without it, each container auto-generates a different random secret, "
            "causing 'Signature verification failed' errors."
        )

    def test_jwt_secret_is_shared_not_per_service(self):
        """JWT secret must be in x-airflow-common (shared), not duplicated per service."""
        compose = _load_compose()
        common_env = _get_compose_shared_env(compose)
        assert "AIRFLOW__API_AUTH__JWT_SECRET" in common_env, (
            "AIRFLOW__API_AUTH__JWT_SECRET must be in x-airflow-common environment "
            "(shared across all Airflow services), not set per-service"
        )

    def test_executor_is_local(self):
        """Docker-compose should use LocalExecutor (no Redis/Celery needed)."""
        compose = _load_compose()
        env = _get_compose_shared_env(compose)
        executor = env.get("AIRFLOW__CORE__EXECUTOR", "")
        assert executor == "LocalExecutor", (
            f"docker-compose should use LocalExecutor, got: {executor}"
        )

    def test_no_celery_vars_in_compose(self):
        """LocalExecutor compose must NOT have Celery env vars."""
        compose = _load_compose()
        env_keys = _get_compose_shared_env_keys(compose)
        celery_vars = {k for k in env_keys if "CELERY" in k}
        assert not celery_vars, (
            f"docker-compose uses LocalExecutor but has Celery env vars: "
            f"{sorted(celery_vars)}. Remove them."
        )

    def test_no_redis_service_in_compose(self):
        """LocalExecutor compose should not include a Redis service."""
        compose = _load_compose()
        services = compose.get("services", {})
        assert "redis" not in services, (
            "docker-compose uses LocalExecutor — Redis service is unnecessary"
        )


class TestComposeHealthchecks:
    """Healthcheck configuration must be correct."""

    def test_postgres_healthcheck_uses_pg_isready(self):
        compose = _load_compose()
        service = compose["services"].get("postgres", {})
        hc = service.get("healthcheck", {})
        test_cmd = str(hc.get("test", ""))
        assert "pg_isready" in test_cmd, (
            "postgres healthcheck must use pg_isready"
        )

    def test_init_depends_on_postgres_healthy(self):
        """airflow-init must wait for postgres to be healthy."""
        compose = _load_compose()
        service = compose["services"].get("airflow-init", {})
        depends = service.get("depends_on", {})
        # Check postgres dependency with condition
        pg_dep = depends.get("postgres", {})
        assert pg_dep.get("condition") == "service_healthy", (
            "airflow-init must depend on postgres with condition: service_healthy"
        )

    def test_webserver_depends_on_init_completed(self):
        """airflow-webserver must wait for airflow-init to complete."""
        compose = _load_compose()
        service = compose["services"].get("airflow-webserver", {})
        depends = service.get("depends_on", {})
        init_dep = depends.get("airflow-init", {})
        assert init_dep.get("condition") == "service_completed_successfully", (
            "airflow-webserver must depend on airflow-init with "
            "condition: service_completed_successfully"
        )

    def test_scheduler_depends_on_init_completed(self):
        compose = _load_compose()
        service = compose["services"].get("airflow-scheduler", {})
        depends = service.get("depends_on", {})
        init_dep = depends.get("airflow-init", {})
        assert init_dep.get("condition") == "service_completed_successfully", (
            "airflow-scheduler must depend on airflow-init with "
            "condition: service_completed_successfully"
        )
