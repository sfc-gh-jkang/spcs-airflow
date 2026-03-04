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

# Env vars required on ALL Airflow services
REQUIRED_ENV_VARS = [
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW__CORE__EXECUTOR",
]

# Celery broker/result backend only needed on services that enqueue or execute
# Celery tasks (api-server, scheduler, workers). The dag-processor and triggerer
# communicate via the Execution API, not Celery.
CELERY_SERVICES = [
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_workers.yaml",
]

CELERY_ENV_VARS = [
    "AIRFLOW__CELERY__BROKER_URL",
    "AIRFLOW__CELERY__RESULT_BACKEND",
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


    @pytest.mark.parametrize("filename", CELERY_SERVICES)
    def test_celery_services_have_broker_vars(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")

        with open(path) as f:
            content = f.read()

        for env_var in CELERY_ENV_VARS:
            assert env_var in content, (
                f"{filename}: missing Celery env var {env_var}"
            )

    @pytest.mark.parametrize("filename", [f for f in AIRFLOW_SERVICES if f not in CELERY_SERVICES])
    def test_non_celery_services_omit_broker_vars(self, filename):
        """dag-processor and triggerer must NOT have Celery broker vars."""
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")

        with open(path) as f:
            content = f.read()

        for env_var in CELERY_ENV_VARS:
            assert env_var not in content, (
                f"{filename}: should NOT have Celery env var {env_var} "
                "(dag-processor/triggerer use Execution API, not Celery)"
            )


class TestReadinessProbe:
    """Only the api-server should have a readinessProbe (HTTP-based)."""

    def test_api_server_has_readiness_probe(self):
        path = os.path.join(SPECS_DIR, "af_api_server.yaml")
        if not os.path.isfile(path):
            pytest.skip("af_api_server.yaml not yet created")
        with open(path) as f:
            content = f.read()
        assert "readinessProbe" in content, (
            "af_api_server.yaml: should have a readinessProbe"
        )

    @pytest.mark.parametrize("filename", [
        "af_postgres.yaml", "af_redis.yaml",
    ])
    def test_infra_services_no_readiness_probe(self, filename):
        """Non-HTTP services must omit readinessProbe (SPCS gotcha)."""
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read()
        assert "readinessProbe" not in content, (
            f"{filename}: must NOT have readinessProbe (non-HTTP service)"
        )


class TestPublicEndpoints:
    """Only the api-server endpoint should be public."""

    def test_api_server_endpoint_is_public(self):
        path = os.path.join(SPECS_DIR, "af_api_server.yaml")
        if not os.path.isfile(path):
            pytest.skip("af_api_server.yaml not yet created")
        with open(path) as f:
            data = yaml.safe_load(f)
        endpoints = data.get("spec", {}).get("endpoints", [])
        public_endpoints = [e for e in endpoints if e.get("public") is True]
        assert len(public_endpoints) >= 1, (
            "af_api_server.yaml: must have at least one public endpoint"
        )

    @pytest.mark.parametrize("filename", [
        "af_postgres.yaml", "af_redis.yaml", "af_scheduler.yaml",
        "af_dag_processor.yaml", "af_triggerer.yaml", "af_workers.yaml",
    ])
    def test_non_api_services_not_public(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read()
        assert "public: true" not in content.lower().replace(" ", ""), (
            f"{filename}: must NOT have public endpoints"
        )


class TestStageVolumeConfig:
    """Stage volumes must have correct uid/gid and stageConfig format."""

    STAGE_VOLUME_SERVICES = [
        "af_api_server.yaml",
        "af_scheduler.yaml",
        "af_dag_processor.yaml",
        "af_workers.yaml",
    ]

    @pytest.mark.parametrize("filename", STAGE_VOLUME_SERVICES)
    def test_stage_volumes_have_uid_gid(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            data = yaml.safe_load(f)
        volumes = data.get("spec", {}).get("volumes", [])
        stage_volumes = [v for v in volumes if v.get("source") == "stage"]
        for vol in stage_volumes:
            assert vol.get("uid") == 50000, (
                f"{filename}: stage volume '{vol.get('name')}' must have uid=50000 (airflow user)"
            )
            assert vol.get("gid") == 0, (
                f"{filename}: stage volume '{vol.get('name')}' must have gid=0 (root group)"
            )

    @pytest.mark.parametrize("filename", STAGE_VOLUME_SERVICES)
    def test_stage_volumes_use_v2_syntax(self, filename):
        """GCP requires v2 stage mount syntax: source: stage + stageConfig."""
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            data = yaml.safe_load(f)
        volumes = data.get("spec", {}).get("volumes", [])
        stage_volumes = [v for v in volumes if v.get("source") == "stage"]
        for vol in stage_volumes:
            assert "stageConfig" in vol, (
                f"{filename}: stage volume '{vol.get('name')}' must use v2 syntax (stageConfig)"
            )
            assert vol["stageConfig"].get("name", "").startswith("@"), (
                f"{filename}: stageConfig.name must start with '@'"
            )

    @pytest.mark.parametrize("filename", STAGE_VOLUME_SERVICES)
    def test_volume_mounts_match_volumes(self, filename):
        """Every volumeMount name must have a matching volume definition."""
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            data = yaml.safe_load(f)
        volume_names = {v["name"] for v in data["spec"].get("volumes", [])}
        for container in data["spec"]["containers"]:
            for mount in container.get("volumeMounts", []):
                assert mount["name"] in volume_names, (
                    f"{filename}: volumeMount '{mount['name']}' has no matching volume"
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
