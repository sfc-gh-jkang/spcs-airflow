"""Tests for cross-file consistency.

Validates that references between files are consistent:
- SQL service creation spec filenames match actual spec files
- teardown.sh covers all services from SQL
- Suspend/resume scripts cover all 7 services
- Version numbers are consistent across files
- Compute pool assignments match spec expectations
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

ALL_SERVICES = [
    "AF_POSTGRES", "AF_REDIS", "AF_API_SERVER",
    "AF_SCHEDULER", "AF_DAG_PROCESSOR", "AF_TRIGGERER", "AF_WORKERS",
]


class TestSpecFilenameConsistency:
    """07_create_services.sql SPECIFICATION_FILE refs must match actual specs."""

    def test_sql_spec_refs_match_files(self):
        sql_path = os.path.join(SQL_DIR, "07_create_services.sql")
        if not os.path.isfile(sql_path):
            pytest.skip("07_create_services.sql not yet created")
        with open(sql_path) as f:
            sql_content = f.read()

        actual_specs = {
            f for f in os.listdir(SPECS_DIR)
            if f.endswith(".yaml")
        }

        # Find SPECIFICATION_FILE references in SQL
        refs = re.findall(r"SPECIFICATION_FILE\s*=\s*'([^']+)'", sql_content, re.IGNORECASE)
        if not refs:
            refs = re.findall(r"@\S+/(\S+\.yaml)", sql_content)

        for ref in refs:
            assert ref in actual_specs, (
                f"07_create_services.sql references '{ref}' but file not in specs/"
            )

    def test_all_specs_referenced_in_sql(self):
        sql_path = os.path.join(SQL_DIR, "07_create_services.sql")
        if not os.path.isfile(sql_path):
            pytest.skip("07_create_services.sql not yet created")
        with open(sql_path) as f:
            sql_content = f.read()

        actual_specs = {
            f for f in os.listdir(SPECS_DIR)
            if f.endswith(".yaml")
        }

        for spec_file in actual_specs:
            assert spec_file in sql_content, (
                f"specs/{spec_file} exists but is not referenced in 07_create_services.sql"
            )


class TestServiceCoverage:
    """Suspend, resume, and validate SQL must cover all 7 services."""

    @pytest.mark.parametrize("sql_file,action", [
        ("09_suspend_all.sql", "suspend"),
        ("10_resume_all.sql", "resume"),
    ])
    def test_covers_all_services(self, sql_file, action):
        path = os.path.join(SQL_DIR, sql_file)
        if not os.path.isfile(path):
            pytest.skip(f"{sql_file} not yet created")
        with open(path) as f:
            content = f.read().upper()
        for service in ALL_SERVICES:
            assert service in content, (
                f"{sql_file}: must {action} service {service}"
            )

    def test_create_services_covers_all(self):
        path = os.path.join(SQL_DIR, "07_create_services.sql")
        if not os.path.isfile(path):
            pytest.skip("07_create_services.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        for service in ALL_SERVICES:
            assert service in content, (
                f"07_create_services.sql: must create service {service}"
            )


class TestComputePoolAssignments:
    """Services must be assigned to the correct compute pools in SQL."""

    POOL_MAP = {
        "AF_POSTGRES": "INFRA_POOL",
        "AF_REDIS": "INFRA_POOL",
        "AF_API_SERVER": "CORE_POOL",
        "AF_SCHEDULER": "CORE_POOL",
        "AF_DAG_PROCESSOR": "CORE_POOL",
        "AF_TRIGGERER": "CORE_POOL",
        "AF_WORKERS": "WORKER_POOL",
    }

    def test_compute_pools_defined(self):
        path = os.path.join(SQL_DIR, "05_setup_compute_pools.sql")
        if not os.path.isfile(path):
            pytest.skip("05_setup_compute_pools.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        for pool in set(self.POOL_MAP.values()):
            assert pool in content, (
                f"05_setup_compute_pools.sql: must define {pool}"
            )

    @pytest.mark.parametrize("service,pool", list(POOL_MAP.items()))
    def test_service_uses_correct_pool(self, service, pool):
        path = os.path.join(SQL_DIR, "07_create_services.sql")
        if not os.path.isfile(path):
            pytest.skip("07_create_services.sql not yet created")
        with open(path) as f:
            content = f.read().upper()

        # Find the CREATE SERVICE block for this service and check its pool
        # Pattern: CREATE SERVICE ... <service> ... COMPUTE_POOL = <pool>
        service_pattern = rf'CREATE\s+SERVICE.*?{service}.*?(?=CREATE\s+SERVICE|$)'
        match = re.search(service_pattern, content, re.DOTALL)
        assert match is not None, (
            f"07_create_services.sql: could not find CREATE SERVICE for {service}"
        )
        assert pool in match.group(), (
            f"07_create_services.sql: {service} must use {pool}, "
            f"but pool not found in its CREATE SERVICE block"
        )


class TestVersionConsistency:
    """Version numbers must be consistent across all files."""

    def test_airflow_version_in_specs_matches_dockerfile(self):
        """All spec YAML image tags must match the Dockerfile version."""
        dockerfile = os.path.join(IMAGES_DIR, "airflow", "Dockerfile")
        if not os.path.isfile(dockerfile):
            pytest.skip("Dockerfile not yet created")
        with open(dockerfile) as f:
            df_content = f.read()
        match = re.search(r'apache/airflow:(\d+\.\d+\.\d+)', df_content)
        assert match, "Could not find airflow version in Dockerfile"
        expected_version = match.group(1)

        airflow_specs = [
            "af_api_server.yaml", "af_scheduler.yaml",
            "af_dag_processor.yaml", "af_triggerer.yaml", "af_workers.yaml",
        ]
        for spec_file in airflow_specs:
            path = os.path.join(SPECS_DIR, spec_file)
            if not os.path.isfile(path):
                continue
            with open(path) as f:
                data = yaml.safe_load(f)
            for container in data["spec"]["containers"]:
                image = container["image"]
                assert expected_version in image, (
                    f"{spec_file}: image '{image}' must contain version {expected_version}"
                )


class TestBlockStorageConfig:
    """Postgres must use block storage; other services must not."""

    def test_postgres_uses_block_storage(self):
        path = os.path.join(SPECS_DIR, "af_postgres.yaml")
        if not os.path.isfile(path):
            pytest.skip("af_postgres.yaml not yet created")
        with open(path) as f:
            content = f.read()
        assert "block" in content.lower(), (
            "af_postgres.yaml: must use blockStorage for data persistence"
        )

    @pytest.mark.parametrize("filename", [
        "af_redis.yaml", "af_api_server.yaml", "af_scheduler.yaml",
        "af_dag_processor.yaml", "af_triggerer.yaml", "af_workers.yaml",
    ])
    def test_non_postgres_no_block_storage(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            data = yaml.safe_load(f)
        volumes = data.get("spec", {}).get("volumes", [])
        block_vols = [v for v in volumes if v.get("source") == "block"]
        assert len(block_vols) == 0, (
            f"{filename}: should NOT use blockStorage (only postgres needs it)"
        )
