"""Tests for multi-container configuration consistency.

Validates that configuration values that MUST be identical across multiple
SPCS service specs are actually identical. Existing tests check that keys
are present; these tests check that the VALUES match.

This catches the class of bugs where someone edits one spec but forgets to
update the others — e.g. pointing one service's FERNET_KEY at a different
secret than the rest, or mounting DAGs to a different path in one spec.
"""

import os
import re

import pytest
import yaml

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
SPECS_DIR = os.path.join(PROJECT_ROOT, "specs")
IMAGES_DIR = os.path.join(PROJECT_ROOT, "images")

# All 5 Airflow service specs (excludes postgres and redis)
AIRFLOW_SPECS = [
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_dag_processor.yaml",
    "af_triggerer.yaml",
    "af_workers.yaml",
]

# Services that write task/scheduler logs to the shared log stage
LOG_VOLUME_SERVICES = [
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_workers.yaml",
]

# Services that do NOT need log volumes (write to stdout only)
NO_LOG_VOLUME_SERVICES = [
    "af_dag_processor.yaml",
    "af_triggerer.yaml",
]


def _load_spec(filename):
    """Load and parse a spec YAML file."""
    path = os.path.join(SPECS_DIR, filename)
    if not os.path.isfile(path):
        pytest.skip(f"{filename} not found")
    with open(path) as f:
        return yaml.safe_load(f)


def _get_env_dict(spec_data):
    """Extract the env dict from the first container in a spec."""
    containers = spec_data.get("spec", {}).get("containers", [])
    if not containers:
        return {}
    env = containers[0].get("env", {})
    if isinstance(env, dict):
        return env
    # Handle list-of-dicts format
    if isinstance(env, list):
        return {item["name"]: item.get("value", "") for item in env if isinstance(item, dict)}
    return {}


def _get_env_value(filename, key):
    """Get a specific env var value from a spec."""
    data = _load_spec(filename)
    env = _get_env_dict(data)
    return env.get(key)


def _get_volume_mounts(spec_data):
    """Extract volumeMounts from the first container."""
    containers = spec_data.get("spec", {}).get("containers", [])
    if not containers:
        return []
    return containers[0].get("volumeMounts", [])


def _get_volumes(spec_data):
    """Extract volumes from a spec."""
    return spec_data.get("spec", {}).get("volumes", [])


# --- Test Classes ---


class TestEnvVarValueConsistency:
    """Env vars that must be identical across all Airflow specs."""

    # These env vars must have the SAME value in every Airflow spec that has them.
    # A mismatch means containers can't communicate or decrypt each other's data.
    MUST_MATCH_VARS = [
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW__CORE__FERNET_KEY",
        "AIRFLOW__API_AUTH__JWT_SECRET",
        "AIRFLOW__CORE__DAGS_FOLDER",
        "AIRFLOW__CORE__EXECUTION_API_SERVER_URL",
    ]

    @pytest.mark.parametrize("env_var", MUST_MATCH_VARS)
    def test_value_identical_across_specs(self, env_var):
        """Every Airflow spec that defines this var must use the same value."""
        values = {}
        for filename in AIRFLOW_SPECS:
            val = _get_env_value(filename, env_var)
            if val is not None:
                values[filename] = val

        if len(values) < 2:
            pytest.skip(f"Fewer than 2 specs define {env_var}")

        unique_values = set(str(v) for v in values.values())
        assert len(unique_values) == 1, (
            f"{env_var} has different values across specs:\n"
            + "\n".join(f"  {f}: {v}" for f, v in sorted(values.items()))
        )


class TestExecutionApiUrlConsistency:
    """EXECUTION_API_SERVER_URL must be correct and consistent."""

    def test_url_is_identical_across_specs(self):
        values = {}
        for filename in AIRFLOW_SPECS:
            val = _get_env_value(filename, "AIRFLOW__CORE__EXECUTION_API_SERVER_URL")
            if val is not None:
                values[filename] = val

        unique = set(values.values())
        assert len(unique) == 1, (
            f"EXECUTION_API_SERVER_URL differs across specs:\n"
            + "\n".join(f"  {f}: {v}" for f, v in sorted(values.items()))
        )

    def test_url_pattern_is_correct(self):
        """URL must follow http://<api-server-dns>:8080/execution/ pattern."""
        for filename in AIRFLOW_SPECS:
            val = _get_env_value(filename, "AIRFLOW__CORE__EXECUTION_API_SERVER_URL")
            if val is None:
                continue
            assert val == "http://af-api-server:8080/execution/", (
                f"{filename}: EXECUTION_API_SERVER_URL must be "
                f"'http://af-api-server:8080/execution/', got: '{val}'"
            )


class TestSecretReferenceConsistency:
    """Secret template references must use the exact same string across specs."""

    # Map of secret name to the env vars that reference it
    SECRET_REFERENCES = {
        "airflow_fernet_key": "AIRFLOW__CORE__FERNET_KEY",
        "airflow_jwt_secret": "AIRFLOW__API_AUTH__JWT_SECRET",
        "airflow_postgres_pwd": "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    }

    @pytest.mark.parametrize("secret_name,env_var", list(SECRET_REFERENCES.items()))
    def test_secret_template_consistent(self, secret_name, env_var):
        """All specs referencing this secret must use the same template string."""
        values = {}
        for filename in AIRFLOW_SPECS:
            val = _get_env_value(filename, env_var)
            if val is not None and secret_name in str(val):
                values[filename] = str(val)

        if len(values) < 2:
            pytest.skip(f"Fewer than 2 specs reference secret {secret_name}")

        unique = set(values.values())
        assert len(unique) == 1, (
            f"Secret '{secret_name}' referenced inconsistently via {env_var}:\n"
            + "\n".join(f"  {f}: {v}" for f, v in sorted(values.items()))
        )

    def test_all_secrets_use_correct_template_syntax(self):
        """Secret refs must use {{secret.<name>.secret_string}} syntax."""
        pattern = re.compile(r"\{\{secret\.\w+\.secret_string\}\}")
        for filename in AIRFLOW_SPECS:
            data = _load_spec(filename)
            env = _get_env_dict(data)
            for key, val in env.items():
                val_str = str(val)
                if "secret." in val_str:
                    assert pattern.search(val_str), (
                        f"{filename}: env var {key} has malformed secret reference: "
                        f"'{val_str}' — expected {{{{secret.<name>.secret_string}}}}"
                    )


class TestVolumeMountConsistency:
    """Volume mount paths and stage names must be identical across specs."""

    def _get_dags_mount_path(self, filename):
        data = _load_spec(filename)
        for mount in _get_volume_mounts(data):
            if "dag" in mount.get("name", "").lower():
                return mount.get("mountPath")
        return None

    def _get_logs_mount_path(self, filename):
        data = _load_spec(filename)
        for mount in _get_volume_mounts(data):
            if "log" in mount.get("name", "").lower():
                return mount.get("mountPath")
        return None

    def _get_stage_name(self, filename, volume_name_contains):
        data = _load_spec(filename)
        for vol in _get_volumes(data):
            if volume_name_contains in vol.get("name", ""):
                config = vol.get("stageConfig", {})
                return config.get("name")
        return None

    def test_dags_mount_path_consistent(self):
        """All specs with DAG volumes must mount to the same path."""
        paths = {}
        for filename in AIRFLOW_SPECS:
            p = self._get_dags_mount_path(filename)
            if p is not None:
                paths[filename] = p

        assert len(paths) >= 2, "Fewer than 2 specs have DAG volume mounts"
        unique = set(paths.values())
        assert len(unique) == 1, (
            f"DAGs mounted to different paths:\n"
            + "\n".join(f"  {f}: {p}" for f, p in sorted(paths.items()))
        )

    def test_dags_stage_name_consistent(self):
        """All specs must reference the same DAGs stage."""
        stages = {}
        for filename in AIRFLOW_SPECS:
            s = self._get_stage_name(filename, "dag")
            if s is not None:
                stages[filename] = s

        assert len(stages) >= 2, "Fewer than 2 specs have DAG stage volumes"
        unique = set(stages.values())
        assert len(unique) == 1, (
            f"DAGs stage name differs across specs:\n"
            + "\n".join(f"  {f}: {s}" for f, s in sorted(stages.items()))
        )

    def test_logs_mount_path_consistent(self):
        """All specs with log volumes must mount to the same path."""
        paths = {}
        for filename in LOG_VOLUME_SERVICES:
            p = self._get_logs_mount_path(filename)
            if p is not None:
                paths[filename] = p

        assert len(paths) >= 2, "Fewer than 2 specs have log volume mounts"
        unique = set(paths.values())
        assert len(unique) == 1, (
            f"Logs mounted to different paths:\n"
            + "\n".join(f"  {f}: {p}" for f, p in sorted(paths.items()))
        )

    def test_logs_stage_name_consistent(self):
        """All specs with log volumes must reference the same logs stage."""
        stages = {}
        for filename in LOG_VOLUME_SERVICES:
            s = self._get_stage_name(filename, "log")
            if s is not None:
                stages[filename] = s

        assert len(stages) >= 2, "Fewer than 2 specs have log stage volumes"
        unique = set(stages.values())
        assert len(unique) == 1, (
            f"Logs stage name differs across specs:\n"
            + "\n".join(f"  {f}: {s}" for f, s in sorted(stages.items()))
        )


class TestLogVolumeAssignment:
    """Only services that write logs should have log volumes."""

    @pytest.mark.parametrize("filename", LOG_VOLUME_SERVICES)
    def test_log_service_has_log_volume(self, filename):
        data = _load_spec(filename)
        volume_names = [v.get("name", "") for v in _get_volumes(data)]
        has_log = any("log" in name.lower() for name in volume_names)
        assert has_log, (
            f"{filename}: must have a log volume (writes task/scheduler logs)"
        )

    @pytest.mark.parametrize("filename", NO_LOG_VOLUME_SERVICES)
    def test_non_log_service_no_log_volume(self, filename):
        data = _load_spec(filename)
        volume_names = [v.get("name", "") for v in _get_volumes(data)]
        has_log = any("log" in name.lower() for name in volume_names)
        assert not has_log, (
            f"{filename}: should NOT have a log volume (writes to stdout only)"
        )


class TestDagsFolderMatchesMountPath:
    """AIRFLOW__CORE__DAGS_FOLDER must match the actual DAGs volume mountPath."""

    @pytest.mark.parametrize("filename", AIRFLOW_SPECS)
    def test_dags_folder_matches_mount(self, filename):
        data = _load_spec(filename)
        env = _get_env_dict(data)
        dags_folder = env.get("AIRFLOW__CORE__DAGS_FOLDER")
        if dags_folder is None:
            pytest.skip(f"{filename}: no DAGS_FOLDER env var")

        # Find the DAGs volume mount path
        mount_path = None
        for mount in _get_volume_mounts(data):
            if "dag" in mount.get("name", "").lower():
                mount_path = mount.get("mountPath")
                break

        if mount_path is None:
            pytest.skip(f"{filename}: no DAGs volume mount")

        assert str(dags_folder) == mount_path, (
            f"{filename}: AIRFLOW__CORE__DAGS_FOLDER='{dags_folder}' does not match "
            f"DAGs volumeMount mountPath='{mount_path}'"
        )


class TestEntrypointRoleAlignment:
    """Each spec's AIRFLOW_ROLE must be a valid entrypoint role."""

    ENTRYPOINT_PATH = os.path.join(IMAGES_DIR, "airflow", "entrypoint.sh")

    # Expected mapping from spec filename to AIRFLOW_ROLE value
    EXPECTED_ROLES = {
        "af_api_server.yaml": "api-server",
        "af_scheduler.yaml": "scheduler",
        "af_dag_processor.yaml": "dag-processor",
        "af_triggerer.yaml": "triggerer",
        "af_workers.yaml": "worker",
    }

    def _get_entrypoint_roles(self):
        """Parse valid roles from the entrypoint.sh case statement."""
        if not os.path.isfile(self.ENTRYPOINT_PATH):
            pytest.skip("entrypoint.sh not found")
        with open(self.ENTRYPOINT_PATH) as f:
            content = f.read()
        # Extract roles from case patterns like: api-server) or worker)
        roles = re.findall(r'^\s+(\S+)\)', content, re.MULTILINE)
        # Filter out catch-all patterns
        return {r for r in roles if r != "*"}

    @pytest.mark.parametrize("filename", AIRFLOW_SPECS)
    def test_spec_has_airflow_role(self, filename):
        """Every Airflow spec must define AIRFLOW_ROLE."""
        data = _load_spec(filename)
        env = _get_env_dict(data)
        assert "AIRFLOW_ROLE" in env, (
            f"{filename}: must define AIRFLOW_ROLE env var"
        )

    @pytest.mark.parametrize("filename,expected_role", list(EXPECTED_ROLES.items()))
    def test_role_is_valid_and_correct(self, filename, expected_role):
        """Each spec's AIRFLOW_ROLE must match its expected role and be handled by entrypoint."""
        valid_roles = self._get_entrypoint_roles()
        data = _load_spec(filename)
        env = _get_env_dict(data)
        actual_role = env.get("AIRFLOW_ROLE")

        assert actual_role == expected_role, (
            f"{filename}: AIRFLOW_ROLE should be '{expected_role}', got '{actual_role}'"
        )
        assert actual_role in valid_roles, (
            f"{filename}: AIRFLOW_ROLE='{actual_role}' is not handled by entrypoint.sh. "
            f"Valid roles: {sorted(valid_roles)}"
        )
