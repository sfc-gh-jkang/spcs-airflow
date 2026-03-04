"""Tests for images/airflow/pyproject.toml.

Validates:
- Correct TOML structure
- Version matches Airflow base image
- Required providers are declared
- All dependency versions are pinned (not loose)
- requires-python matches base image
"""

import os
import re

import pytest

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
PYPROJECT = os.path.join(PROJECT_ROOT, "images", "airflow", "pyproject.toml")
DOCKERFILE = os.path.join(PROJECT_ROOT, "images", "airflow", "Dockerfile")

REQUIRED_PROVIDERS = [
    "apache-airflow-providers-celery",
    "apache-airflow-providers-postgres",
    "apache-airflow-providers-redis",
    "apache-airflow-providers-snowflake",
]


@pytest.fixture
def pyproject_content():
    if not os.path.isfile(PYPROJECT):
        pytest.skip("pyproject.toml not yet created")
    with open(PYPROJECT) as f:
        return f.read()


class TestPyprojectStructure:
    """pyproject.toml must have required TOML sections."""

    def test_has_project_section(self, pyproject_content):
        assert "[project]" in pyproject_content, (
            "pyproject.toml: must have [project] section"
        )

    def test_has_dependencies(self, pyproject_content):
        assert "dependencies" in pyproject_content, (
            "pyproject.toml: must declare dependencies"
        )

    def test_has_requires_python(self, pyproject_content):
        assert "requires-python" in pyproject_content, (
            "pyproject.toml: must declare requires-python"
        )

    def test_requires_python_312(self, pyproject_content):
        assert "3.12" in pyproject_content, (
            "pyproject.toml: requires-python should reference 3.12 "
            "(matching the base image)"
        )


class TestPyprojectProviders:
    """All required Airflow providers must be declared."""

    @pytest.mark.parametrize("provider", REQUIRED_PROVIDERS)
    def test_has_provider(self, pyproject_content, provider):
        assert provider in pyproject_content, (
            f"pyproject.toml: must include '{provider}'"
        )


class TestPyprojectVersionPinning:
    """All dependencies must have pinned versions (==)."""

    def test_all_deps_pinned(self, pyproject_content):
        """Every dependency line must use == pinning."""
        in_deps = False
        for line in pyproject_content.splitlines():
            stripped = line.strip()
            if stripped.startswith("dependencies"):
                in_deps = True
                continue
            if in_deps and stripped == "]":
                break
            if in_deps and stripped.startswith('"') and stripped.endswith('",'):
                dep = stripped.strip('"').rstrip('",')
                if dep:  # skip empty
                    assert "==" in dep, (
                        f"pyproject.toml: dependency '{dep}' must be pinned with =="
                    )


class TestPyprojectVersionConsistency:
    """pyproject.toml version should match the Dockerfile base image."""

    def test_version_matches_dockerfile(self, pyproject_content):
        if not os.path.isfile(DOCKERFILE):
            pytest.skip("Dockerfile not yet created")
        with open(DOCKERFILE) as f:
            dockerfile = f.read()
        # Extract version from FROM apache/airflow:X.Y.Z
        match = re.search(r'apache/airflow:(\d+\.\d+\.\d+)', dockerfile)
        assert match, "Dockerfile: could not find airflow version in FROM line"
        dockerfile_version = match.group(1)
        assert f'version = "{dockerfile_version}"' in pyproject_content, (
            f"pyproject.toml version must match Dockerfile ({dockerfile_version})"
        )


class TestNoRequirementsTxt:
    """requirements.txt must not exist (replaced by pyproject.toml)."""

    def test_requirements_txt_deleted(self):
        req_path = os.path.join(PROJECT_ROOT, "images", "airflow", "requirements.txt")
        assert not os.path.exists(req_path), (
            "images/airflow/requirements.txt must be deleted "
            "(replaced by pyproject.toml + UV)"
        )
