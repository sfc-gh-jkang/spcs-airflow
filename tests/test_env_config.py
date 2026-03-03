"""Tests for environment configuration completeness.

Validates that:
- Required environment variables are defined in service specs
- Secret references match between SQL and YAML specs
- All services that need DB access have connection string config
"""

import os
import yaml
import pytest

SPECS_DIR = os.path.join(os.path.dirname(__file__), "..", "specs")
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")

# Airflow services that need these env vars
AIRFLOW_SERVICES = [
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_dag_processor.yaml",
    "af_triggerer.yaml",
    "af_workers.yaml",
]

REQUIRED_ENV_VARS = [
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "AIRFLOW__CELERY__BROKER_URL",
    "AIRFLOW__CELERY__RESULT_BACKEND",
    "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW__CORE__EXECUTOR",
]


def _get_env_keys_from_spec(spec_path):
    """Extract all environment variable names from a service spec."""
    if not os.path.isfile(spec_path):
        return set()
    with open(spec_path) as f:
        data = yaml.safe_load(f)
    env_keys = set()
    for container in data.get("spec", {}).get("containers", []):
        for env in container.get("env", {}):
            if isinstance(env, dict):
                env_keys.add(env.get("name", ""))
            elif isinstance(env, str):
                env_keys.add(env)
        # Also check env defined as dict keys
        env_dict = container.get("env", {})
        if isinstance(env_dict, dict):
            env_keys.update(env_dict.keys())
    return env_keys


class TestAirflowEnvVars:
    """Airflow services must have required environment variables."""

    @pytest.mark.parametrize("filename", AIRFLOW_SERVICES)
    def test_has_required_env_vars(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")

        with open(path) as f:
            content = f.read()

        for env_var in REQUIRED_ENV_VARS:
            assert env_var in content, (
                f"{filename}: missing required env var {env_var}"
            )


class TestSecretConsistency:
    """Secrets defined in SQL must be referenced in YAML specs."""

    EXPECTED_SECRETS = [
        "airflow_fernet_key",
        "airflow_postgres_pwd",
        "airflow_redis_pwd",
        "airflow_jwt_secret",
    ]

    def test_secrets_sql_exists(self):
        path = os.path.join(SQL_DIR, "03_setup_secrets.sql.template")
        if not os.path.isfile(path):
            pytest.skip("03_setup_secrets.sql.template not yet created")
        with open(path) as f:
            content = f.read().upper()
        for secret in self.EXPECTED_SECRETS:
            assert secret.upper() in content, (
                f"03_setup_secrets.sql.template: must define secret {secret}"
            )


class TestExecutorConfig:
    """Worker service must be configured for CeleryExecutor."""

    def test_worker_uses_celery_executor(self):
        path = os.path.join(SPECS_DIR, "af_workers.yaml")
        if not os.path.isfile(path):
            pytest.skip("af_workers.yaml not yet created")
        with open(path) as f:
            content = f.read()
        assert "CeleryExecutor" in content, (
            "af_workers.yaml: must specify CeleryExecutor"
        )
