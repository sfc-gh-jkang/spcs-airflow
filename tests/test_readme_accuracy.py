"""Tests for README.md accuracy.

Validates that the README references real files, services, and
configuration that matches the actual project state.
"""

import os

import pytest

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
README = os.path.join(PROJECT_ROOT, "README.md")

ALL_SERVICES = [
    "AF_POSTGRES", "AF_REDIS", "AF_API_SERVER",
    "AF_SCHEDULER", "AF_DAG_PROCESSOR", "AF_TRIGGERER", "AF_WORKERS",
]

EXPECTED_SECRETS = [
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_POSTGRES_PWD",
    "AIRFLOW_REDIS_PWD",
    "AIRFLOW_JWT_SECRET",
]


@pytest.fixture
def readme_content():
    if not os.path.isfile(README):
        pytest.skip("README.md not yet created")
    with open(README) as f:
        return f.read()


class TestReadmeExists:
    """README must exist."""

    def test_readme_exists(self):
        assert os.path.isfile(README), "README.md must exist"


class TestReadmeServices:
    """README must list all 7 services."""

    @pytest.mark.parametrize("service", ALL_SERVICES)
    def test_service_mentioned(self, readme_content, service):
        assert service in readme_content, (
            f"README.md: must mention service {service}"
        )


class TestReadmeSecrets:
    """README must document all 4 secrets."""

    @pytest.mark.parametrize("secret", EXPECTED_SECRETS)
    def test_secret_documented(self, readme_content, secret):
        assert secret in readme_content, (
            f"README.md: must document secret {secret}"
        )


class TestReadmeFileReferences:
    """Files referenced in README must actually exist on disk."""

    REFERENCED_PATHS = [
        "scripts/generate_secrets.sh",
        "scripts/deploy.sh",
        "scripts/build_and_push.sh",
        "scripts/teardown.sh",
        "images/airflow/entrypoint.sh",
        "images/airflow/pyproject.toml",
        "dags/snowflake_etl_pipeline.py",
        "dags/example_snowflake.py",
        "dags/example_taskflow.py",
        "sql/07_create_services.sql",
        "sql/09_suspend_all.sql",
        "sql/10_resume_all.sql",
        "SPECIFICATION.md",
        "CONSTITUTION.md",
    ]

    @pytest.mark.parametrize("rel_path", REFERENCED_PATHS)
    def test_referenced_file_exists(self, rel_path):
        full_path = os.path.join(PROJECT_ROOT, rel_path)
        assert os.path.exists(full_path), (
            f"README references '{rel_path}' but file does not exist"
        )


class TestReadmeContent:
    """README must mention key project details."""

    def test_mentions_uv(self, readme_content):
        assert "UV" in readme_content or "uv" in readme_content, (
            "README.md: must mention UV for dependency management"
        )

    def test_mentions_pyproject_toml(self, readme_content):
        assert "pyproject.toml" in readme_content, (
            "README.md: must mention pyproject.toml"
        )

    def test_does_not_mention_requirements_txt_as_current(self, readme_content):
        """README should not list requirements.txt as the current dep file."""
        # It's OK if it says "not pip/requirements.txt" — just not as a current file
        lines = readme_content.splitlines()
        for line in lines:
            if "requirements.txt" in line and "│" in line:
                # This is a project tree line listing requirements.txt as a file
                pytest.fail(
                    "README.md: project tree still lists requirements.txt "
                    "(should be pyproject.toml)"
                )

    def test_mentions_simple_auth_manager(self, readme_content):
        assert "Simple Auth Manager" in readme_content or "simple_auth_manager" in readme_content, (
            "README.md: must document Simple Auth Manager for Airflow 3.x"
        )

    def test_mentions_celery_executor(self, readme_content):
        assert "CeleryExecutor" in readme_content, (
            "README.md: must mention CeleryExecutor"
        )

    def test_mentions_spcs_gotchas(self, readme_content):
        assert "Gotchas" in readme_content or "gotchas" in readme_content, (
            "README.md: must have SPCS Gotchas section"
        )
