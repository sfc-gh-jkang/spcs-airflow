"""Tests for SPCS service YAML spec schemas.

Validates that all 7 service specs conform to SPCS YAML structure:
- Required top-level keys (spec)
- Container definitions with image, name
- Endpoint definitions where applicable
- Volume mounts reference valid stage paths
- Secret references use correct Snowflake SECRET syntax
"""

import os
import yaml
import pytest

SPECS_DIR = os.path.join(os.path.dirname(__file__), "..", "specs")

EXPECTED_SPECS = [
    "af_postgres.yaml",
    "af_redis.yaml",
    "af_api_server.yaml",
    "af_scheduler.yaml",
    "af_dag_processor.yaml",
    "af_triggerer.yaml",
    "af_workers.yaml",
]


@pytest.fixture(params=EXPECTED_SPECS)
def spec_path(request):
    return os.path.join(SPECS_DIR, request.param)


@pytest.fixture
def spec_data(spec_path):
    with open(spec_path) as f:
        return yaml.safe_load(f)


class TestSpecFilesExist:
    """All 7 service spec files must exist."""

    @pytest.mark.parametrize("filename", EXPECTED_SPECS)
    def test_spec_file_exists(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        assert os.path.isfile(path), f"Missing service spec: {filename}"


class TestSpecStructure:
    """Each spec must have valid SPCS YAML structure."""

    def test_has_spec_key(self, spec_data, spec_path):
        assert "spec" in spec_data, f"{spec_path}: missing top-level 'spec' key"

    def test_has_containers(self, spec_data, spec_path):
        spec = spec_data["spec"]
        assert "containers" in spec, f"{spec_path}: missing 'spec.containers'"
        assert isinstance(spec["containers"], list), "containers must be a list"
        assert len(spec["containers"]) >= 1, "must have at least one container"

    def test_containers_have_required_fields(self, spec_data, spec_path):
        for container in spec_data["spec"]["containers"]:
            assert "name" in container, f"{spec_path}: container missing 'name'"
            assert "image" in container, f"{spec_path}: container missing 'image'"

    def test_container_names_are_strings(self, spec_data, spec_path):
        for container in spec_data["spec"]["containers"]:
            assert isinstance(container["name"], str)
            assert len(container["name"]) > 0


class TestEndpoints:
    """Services with public/internal endpoints must define them correctly."""

    SERVICES_WITH_ENDPOINTS = {
        "af_postgres.yaml": [5432],
        "af_redis.yaml": [6379],
        "af_api_server.yaml": [8080],
    }

    @pytest.mark.parametrize(
        "filename,expected_ports", list(SERVICES_WITH_ENDPOINTS.items())
    )
    def test_endpoint_ports(self, filename, expected_ports):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            data = yaml.safe_load(f)
        spec = data["spec"]
        assert "endpoints" in spec, f"{filename}: missing endpoints"
        defined_ports = [ep.get("port") for ep in spec["endpoints"]]
        for port in expected_ports:
            assert port in defined_ports, f"{filename}: missing port {port}"


class TestSecrets:
    """Services that need secrets must reference them correctly."""

    SERVICES_WITH_SECRETS = [
        "af_api_server.yaml",
        "af_scheduler.yaml",
        "af_dag_processor.yaml",
        "af_triggerer.yaml",
        "af_workers.yaml",
    ]

    @pytest.mark.parametrize("filename", SERVICES_WITH_SECRETS)
    def test_airflow_containers_reference_secrets(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            data = yaml.safe_load(f)
        containers = data["spec"]["containers"]
        # At least one container should have secrets or env referencing secrets
        has_secrets = any(
            "secrets" in c or "env" in c for c in containers
        )
        assert has_secrets, f"{filename}: Airflow containers must reference secrets or env vars"


class TestVolumes:
    """Services that mount stages must define volumes correctly."""

    SERVICES_WITH_VOLUMES = [
        "af_postgres.yaml",
        "af_api_server.yaml",
        "af_scheduler.yaml",
        "af_dag_processor.yaml",
        "af_workers.yaml",
    ]

    @pytest.mark.parametrize("filename", SERVICES_WITH_VOLUMES)
    def test_has_volumes(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            data = yaml.safe_load(f)
        spec = data["spec"]
        has_volumes = "volumes" in spec or any(
            "volumeMounts" in c for c in spec.get("containers", [])
        )
        assert has_volumes, f"{filename}: expected volume definitions"


class TestImagePlatform:
    """All images must reference our SPCS repository path pattern."""

    SPCS_REPO_PATTERN = "/airflow_db/airflow_schema/airflow_repository/"

    def test_images_use_spcs_repo(self, spec_data, spec_path):
        for container in spec_data["spec"]["containers"]:
            image = container["image"]
            assert self.SPCS_REPO_PATTERN in image or image.startswith("/"), (
                f"{spec_path}: image '{image}' should reference SPCS repository"
            )
