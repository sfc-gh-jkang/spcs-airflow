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
        "scripts/sync_dags.sh",
        "scripts/teardown.sh",
        "images/airflow/entrypoint.sh",
        "images/airflow/pyproject.toml",
        "dags/utils/__init__.py",
        "dags/utils/snowflake_conn.py",
        "dags/snowflake_etl_pipeline.py",
        "dags/example_snowflake.py",
        "dags/example_taskflow.py",
        "dags/e2e_snowflake_objects.py",
        "sql/07_create_services.sql",
        "sql/07b_update_services.sql",
        "sql/09_suspend_all.sql",
        "sql/10_resume_all.sql",
        "docker-compose.yaml",
        ".env.example",
        "SPECIFICATION.md",
        "CONSTITUTION.md",
        "tests/test_e2e_spcs.py",
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

    def test_has_dag_deployment_section(self, readme_content):
        assert "Local Development & DAG Deployment" in readme_content or "DAG Deployment" in readme_content, (
            "README.md: must have a DAG deployment section"
        )

    def test_mentions_sync_dags(self, readme_content):
        assert "sync_dags" in readme_content, (
            "README.md: must reference sync_dags.sh for hot-reload workflow"
        )

    def test_documents_what_requires_redeploy(self, readme_content):
        assert "redeploy" in readme_content.lower() or "What requires" in readme_content, (
            "README.md: must document what changes require a full redeploy vs hot-reload"
        )

    def test_has_docker_compose_section(self, readme_content):
        assert "Docker Compose" in readme_content, (
            "README.md: must have a Local Development with Docker Compose section"
        )

    def test_mentions_local_executor(self, readme_content):
        assert "LocalExecutor" in readme_content, (
            "README.md: must mention LocalExecutor for local dev"
        )

    def test_mentions_snowflake_conn_helper(self, readme_content):
        assert "snowflake_conn" in readme_content, (
            "README.md: must reference the shared snowflake_conn helper"
        )

    def test_mentions_env_example(self, readme_content):
        assert ".env.example" in readme_content, (
            "README.md: must reference .env.example for local credentials"
        )

    def test_has_e2e_test_section(self, readme_content):
        assert "End-to-End Tests" in readme_content, (
            "README.md: must document E2E tests against live SPCS"
        )

    def test_mentions_execute_job_service(self, readme_content):
        assert "EXECUTE JOB SERVICE" in readme_content, (
            "README.md: must mention EXECUTE JOB SERVICE for E2E testing"
        )

    def test_documents_update_flag(self, readme_content):
        """README must document the --update flag for preserving ingress URLs."""
        assert "--update" in readme_content, (
            "README.md: must document the --update flag for deploy.sh"
        )

    def test_documents_alter_service(self, readme_content):
        """README must mention ALTER SERVICE as the update mechanism."""
        assert "ALTER SERVICE" in readme_content, (
            "README.md: must mention ALTER SERVICE for updating existing services"
        )
