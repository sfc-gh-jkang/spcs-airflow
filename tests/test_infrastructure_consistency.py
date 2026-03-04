"""Tests for infrastructure-layer consistency across SPCS services.

Validates that infrastructure configuration (Redis/Postgres passwords, image
paths, ports, secret bindings, template placeholders) is consistent across
all files that must agree. Complements test_multi_container_consistency.py
which covers Airflow-config-layer consistency.
"""

import os
import re

import pytest
import yaml

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
SPECS_DIR = os.path.join(PROJECT_ROOT, "specs")
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
IMAGES_DIR = os.path.join(PROJECT_ROOT, "images")

ALL_SPECS = [
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_dag_processor.yaml",
    "af_triggerer.yaml",
    "af_workers.yaml",
    "af_postgres.yaml",
    "af_redis.yaml",
]

AIRFLOW_SPECS = [
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_dag_processor.yaml",
    "af_triggerer.yaml",
    "af_workers.yaml",
]

# Services that use Celery (and therefore need Redis credentials)
CELERY_SERVICES = [
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_workers.yaml",
]


def _load_spec(filename):
    path = os.path.join(SPECS_DIR, filename)
    if not os.path.isfile(path):
        pytest.skip(f"{filename} not found")
    with open(path) as f:
        return yaml.safe_load(f)


def _get_env_dict(spec_data):
    containers = spec_data.get("spec", {}).get("containers", [])
    if not containers:
        return {}
    env = containers[0].get("env", {})
    if isinstance(env, dict):
        return env
    if isinstance(env, list):
        return {item["name"]: item.get("value", "") for item in env if isinstance(item, dict)}
    return {}


def _get_secret_object_names(spec_data):
    """Extract all secret objectNames from a spec's secrets block."""
    containers = spec_data.get("spec", {}).get("containers", [])
    if not containers:
        return set()
    names = set()
    for secret in containers[0].get("secrets", []):
        sf_secret = secret.get("snowflakeSecret", {})
        name = sf_secret.get("objectName")
        if name:
            names.add(name)
    return names


def _extract_secret_name_from_template(value):
    """Extract secret name from {{secret.<name>.secret_string}} pattern."""
    match = re.search(r"\{\{secret\.(\w+)\.secret_string\}\}", str(value))
    return match.group(1) if match else None


# --- Gap 1: Redis Password Consistency ---


class TestRedisPasswordConsistency:
    """Redis server and Celery clients must use the same password secret."""

    def test_redis_and_clients_use_same_secret(self):
        """Redis spec's REDIS_PASSWORD and clients' BROKER_URL must reference
        the same secret (airflow_redis_pwd)."""
        # Get Redis server's password secret
        redis_data = _load_spec("af_redis.yaml")
        redis_env = _get_env_dict(redis_data)
        redis_secret = _extract_secret_name_from_template(
            redis_env.get("REDIS_PASSWORD", "")
        )
        assert redis_secret is not None, (
            "af_redis.yaml: REDIS_PASSWORD must use a secret template"
        )

        # Check each Celery client's BROKER_URL references the same secret
        for filename in CELERY_SERVICES:
            data = _load_spec(filename)
            env = _get_env_dict(data)
            broker_url = str(env.get("AIRFLOW__CELERY__BROKER_URL", ""))
            client_secret = _extract_secret_name_from_template(broker_url)
            assert client_secret == redis_secret, (
                f"{filename}: CELERY__BROKER_URL uses secret '{client_secret}' "
                f"but af_redis.yaml uses '{redis_secret}' — password mismatch"
            )

    def test_redis_spec_declares_password_secret(self):
        """Redis spec's secrets block must include its password secret."""
        data = _load_spec("af_redis.yaml")
        secret_names = _get_secret_object_names(data)
        assert "airflow_redis_pwd" in secret_names, (
            "af_redis.yaml: secrets block must include airflow_redis_pwd"
        )


# --- Gap 2: Postgres Password Consistency ---


class TestPostgresPasswordConsistency:
    """Postgres server and Airflow clients must use the same password secret."""

    def test_postgres_and_clients_use_same_secret(self):
        """Postgres spec's POSTGRES_PASSWORD and clients' SQL_ALCHEMY_CONN must
        reference the same secret."""
        pg_data = _load_spec("af_postgres.yaml")
        pg_env = _get_env_dict(pg_data)
        pg_secret = _extract_secret_name_from_template(
            pg_env.get("POSTGRES_PASSWORD", "")
        )
        assert pg_secret is not None, (
            "af_postgres.yaml: POSTGRES_PASSWORD must use a secret template"
        )

        for filename in AIRFLOW_SPECS:
            data = _load_spec(filename)
            env = _get_env_dict(data)
            conn_str = str(env.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", ""))
            client_secret = _extract_secret_name_from_template(conn_str)
            assert client_secret == pg_secret, (
                f"{filename}: SQL_ALCHEMY_CONN uses secret '{client_secret}' "
                f"but af_postgres.yaml uses '{pg_secret}' — password mismatch"
            )

    def test_postgres_spec_declares_password_secret(self):
        """Postgres spec's secrets block must include its password secret."""
        data = _load_spec("af_postgres.yaml")
        secret_names = _get_secret_object_names(data)
        assert "airflow_postgres_pwd" in secret_names, (
            "af_postgres.yaml: secrets block must include airflow_postgres_pwd"
        )


# --- Gap 3: API Secret Key ↔ JWT Secret Alignment ---


class TestApiSecretKeyAlignment:
    """API__SECRET_KEY and API_AUTH__JWT_SECRET must reference the same value."""

    @pytest.mark.parametrize("filename", AIRFLOW_SPECS)
    def test_api_keys_match(self, filename):
        data = _load_spec(filename)
        env = _get_env_dict(data)
        api_secret = env.get("AIRFLOW__API__SECRET_KEY")
        jwt_secret = env.get("AIRFLOW__API_AUTH__JWT_SECRET")

        if api_secret is None or jwt_secret is None:
            pytest.skip(f"{filename}: missing one of API__SECRET_KEY or JWT_SECRET")

        assert str(api_secret) == str(jwt_secret), (
            f"{filename}: AIRFLOW__API__SECRET_KEY and AIRFLOW__API_AUTH__JWT_SECRET "
            f"must reference the same value.\n"
            f"  API__SECRET_KEY:      {api_secret}\n"
            f"  API_AUTH__JWT_SECRET: {jwt_secret}"
        )


# --- Gap 4: Secret Bindings — specs reference only secrets that exist ---


class TestSecretBindingsInSql:
    """Every secret in a spec's secrets block must be defined in SQL."""

    def _get_sql_secret_names(self):
        """Extract secret names from 03_setup_secrets.sql.template."""
        path = os.path.join(SQL_DIR, "03_setup_secrets.sql.template")
        if not os.path.isfile(path):
            pytest.skip("03_setup_secrets.sql.template not found")
        with open(path) as f:
            content = f.read()
        # Match: CREATE OR REPLACE SECRET <NAME>
        return {
            m.lower()
            for m in re.findall(r"CREATE\s+OR\s+REPLACE\s+SECRET\s+(\w+)", content, re.IGNORECASE)
        }

    @pytest.mark.parametrize("filename", ALL_SPECS)
    def test_spec_secrets_exist_in_sql(self, filename):
        """Every secret referenced in a spec must be defined in the SQL template."""
        sql_secrets = self._get_sql_secret_names()
        data = _load_spec(filename)
        spec_secrets = _get_secret_object_names(data)

        for secret in spec_secrets:
            assert secret.lower() in sql_secrets, (
                f"{filename}: references secret '{secret}' in its secrets block, "
                f"but it's not defined in 03_setup_secrets.sql.template. "
                f"Defined secrets: {sorted(sql_secrets)}"
            )


# --- Gap 5: Image Path Consistency ---


class TestImagePathConsistency:
    """Spec image paths must match what build_and_push.sh builds."""

    BUILD_SCRIPT = os.path.join(SCRIPTS_DIR, "build_and_push.sh")

    def _get_build_script_tags(self):
        """Extract image name:version tags from build_and_push.sh."""
        if not os.path.isfile(self.BUILD_SCRIPT):
            pytest.skip("build_and_push.sh not found")
        with open(self.BUILD_SCRIPT) as f:
            content = f.read()

        # Resolve version variables
        versions = {}
        for match in re.finditer(r'(\w+_VERSION)="([^"]+)"', content):
            versions[match.group(1)] = match.group(2)

        # Extract TAGS array entries like "airflow:${AIRFLOW_VERSION}"
        tags = []
        for match in re.finditer(r'"([^"]+:\$\{(\w+)\})"', content):
            tag_template = match.group(1)
            var_name = match.group(2)
            if var_name in versions:
                tag = tag_template.replace(f"${{{var_name}}}", versions[var_name])
                tags.append(tag)
        return tags

    @pytest.mark.parametrize("filename", ALL_SPECS)
    def test_spec_image_matches_build_script(self, filename):
        """Each spec's image name:tag must match one of build_and_push.sh's TAGS."""
        build_tags = self._get_build_script_tags()
        if not build_tags:
            pytest.skip("Could not parse tags from build_and_push.sh")

        data = _load_spec(filename)
        containers = data.get("spec", {}).get("containers", [])
        for container in containers:
            image = container.get("image", "")
            # Spec image: /db/schema/repo/name:version — extract name:version
            image_tag = image.rsplit("/", 1)[-1] if "/" in image else image
            assert image_tag in build_tags, (
                f"{filename}: image '{image}' has tag '{image_tag}' "
                f"not found in build_and_push.sh TAGS: {build_tags}"
            )

    def test_build_script_versions_match_dockerfiles(self):
        """Version pins in build_and_push.sh must match Dockerfile FROM tags."""
        if not os.path.isfile(self.BUILD_SCRIPT):
            pytest.skip("build_and_push.sh not found")
        with open(self.BUILD_SCRIPT) as f:
            content = f.read()

        version_checks = {
            "AIRFLOW_VERSION": ("images/airflow/Dockerfile", "apache/airflow"),
            "POSTGRES_VERSION": ("images/postgres/Dockerfile", "postgres"),
            "REDIS_VERSION": ("images/redis/Dockerfile", "redis"),
        }

        for var_name, (dockerfile_rel, base_image) in version_checks.items():
            # Extract version from build script
            match = re.search(rf'{var_name}="([^"]+)"', content)
            if not match:
                continue
            script_version = match.group(1)

            # Extract version from Dockerfile
            df_path = os.path.join(PROJECT_ROOT, dockerfile_rel)
            if not os.path.isfile(df_path):
                continue
            with open(df_path) as f:
                df_content = f.read()
            df_match = re.search(rf"FROM\s+{base_image}:(\S+)", df_content)
            if not df_match:
                continue
            df_version = df_match.group(1)

            assert script_version in df_version, (
                f"build_and_push.sh {var_name}='{script_version}' does not match "
                f"{dockerfile_rel} FROM {base_image}:{df_version}"
            )


# --- Gap 6: Postgres Version Safety ---


class TestPostgresVersionSafety:
    """Postgres image must be v17.x (v18 is incompatible with Airflow 3.1.7)."""

    def test_postgres_is_not_v18(self):
        data = _load_spec("af_postgres.yaml")
        containers = data.get("spec", {}).get("containers", [])
        for container in containers:
            image = container.get("image", "")
            assert ":18" not in image, (
                f"af_postgres.yaml: image '{image}' uses PostgreSQL 18 which is "
                f"incompatible with Airflow 3.1.7. Use PostgreSQL 17.x."
            )

    def test_postgres_is_v17(self):
        data = _load_spec("af_postgres.yaml")
        containers = data.get("spec", {}).get("containers", [])
        for container in containers:
            image = container.get("image", "")
            assert ":17" in image, (
                f"af_postgres.yaml: image '{image}' must use PostgreSQL 17.x"
            )


# --- Gap 7: API Server Port Consistency ---


class TestApiServerPortConsistency:
    """API server port must be consistent across entrypoint, spec endpoint,
    readiness probe, and EXECUTION_API_SERVER_URL."""

    EXPECTED_PORT = 8080

    def test_entrypoint_port_matches(self):
        path = os.path.join(IMAGES_DIR, "airflow", "entrypoint.sh")
        if not os.path.isfile(path):
            pytest.skip("entrypoint.sh not found")
        with open(path) as f:
            content = f.read()
        assert f"--port {self.EXPECTED_PORT}" in content, (
            f"entrypoint.sh: api-server must use --port {self.EXPECTED_PORT}"
        )

    def test_spec_endpoint_port_matches(self):
        data = _load_spec("af_api_server.yaml")
        endpoints = data.get("spec", {}).get("endpoints", [])
        ports = [e.get("port") for e in endpoints]
        assert self.EXPECTED_PORT in ports, (
            f"af_api_server.yaml: endpoint port must be {self.EXPECTED_PORT}, "
            f"got: {ports}"
        )

    def test_readiness_probe_port_matches(self):
        data = _load_spec("af_api_server.yaml")
        containers = data.get("spec", {}).get("containers", [])
        for container in containers:
            probe = container.get("readinessProbe", {})
            if probe:
                assert probe.get("port") == self.EXPECTED_PORT, (
                    f"af_api_server.yaml: readinessProbe port must be "
                    f"{self.EXPECTED_PORT}, got: {probe.get('port')}"
                )

    def test_execution_api_url_port_matches(self):
        """EXECUTION_API_SERVER_URL in all specs must use the same port."""
        for filename in AIRFLOW_SPECS:
            data = _load_spec(filename)
            env = _get_env_dict(data)
            url = str(env.get("AIRFLOW__CORE__EXECUTION_API_SERVER_URL", ""))
            assert f":{self.EXPECTED_PORT}/" in url, (
                f"{filename}: EXECUTION_API_SERVER_URL must use port "
                f"{self.EXPECTED_PORT}, got: {url}"
            )


# --- Gap 8: Entrypoint Role Safety ---


class TestEntrypointRoleSafety:
    """Services must not have incorrect AIRFLOW_ROLE values."""

    @pytest.mark.parametrize("filename", ["af_dag_processor.yaml", "af_triggerer.yaml"])
    def test_non_celery_service_not_worker_role(self, filename):
        """dag-processor and triggerer must NOT have AIRFLOW_ROLE=worker."""
        data = _load_spec(filename)
        env = _get_env_dict(data)
        role = env.get("AIRFLOW_ROLE", "")
        assert role != "worker", (
            f"{filename}: AIRFLOW_ROLE must NOT be 'worker' — "
            f"this service doesn't execute Celery tasks"
        )

    @pytest.mark.parametrize("filename", ["af_postgres.yaml", "af_redis.yaml"])
    def test_infra_service_no_airflow_role(self, filename):
        """Infrastructure services must NOT have AIRFLOW_ROLE."""
        data = _load_spec(filename)
        env = _get_env_dict(data)
        assert "AIRFLOW_ROLE" not in env, (
            f"{filename}: infrastructure service must NOT have AIRFLOW_ROLE "
            f"(not an Airflow process)"
        )


# --- Gap 9: Generate Secrets Placeholders ---


class TestGenerateSecretsPlaceholders:
    """generate_secrets.sh substitutions must match template placeholders."""

    TEMPLATE_PATH = os.path.join(SQL_DIR, "03_setup_secrets.sql.template")
    SCRIPT_PATH = os.path.join(SCRIPTS_DIR, "generate_secrets.sh")

    def _get_template_placeholders(self):
        if not os.path.isfile(self.TEMPLATE_PATH):
            pytest.skip("03_setup_secrets.sql.template not found")
        with open(self.TEMPLATE_PATH) as f:
            content = f.read()
        return set(re.findall(r"<CHANGE_ME_\w+>", content))

    def _get_script_substitutions(self):
        if not os.path.isfile(self.SCRIPT_PATH):
            pytest.skip("generate_secrets.sh not found")
        with open(self.SCRIPT_PATH) as f:
            content = f.read()
        return set(re.findall(r"<CHANGE_ME_\w+>", content))

    def test_all_placeholders_have_substitutions(self):
        """Every placeholder in the template must be replaced by the script."""
        placeholders = self._get_template_placeholders()
        substitutions = self._get_script_substitutions()
        missing = placeholders - substitutions
        assert not missing, (
            f"Template has placeholders not handled by generate_secrets.sh: "
            f"{sorted(missing)}"
        )

    def test_all_substitutions_have_placeholders(self):
        """Every substitution in the script must have a matching placeholder."""
        placeholders = self._get_template_placeholders()
        substitutions = self._get_script_substitutions()
        extra = substitutions - placeholders
        assert not extra, (
            f"generate_secrets.sh substitutes placeholders not in template: "
            f"{sorted(extra)}"
        )
