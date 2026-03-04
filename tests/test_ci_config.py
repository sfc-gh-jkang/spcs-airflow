"""Tests for CI/CD configuration and project hygiene files.

Validates:
- .gitlab-ci.yml structure and test stage
- .gitignore covers all sensitive/generated files
"""

import os

import pytest
import yaml

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")


class TestGitlabCi:
    """.gitlab-ci.yml must exist and define a test stage."""

    CI_FILE = os.path.join(PROJECT_ROOT, ".gitlab-ci.yml")

    def test_file_exists(self):
        assert os.path.isfile(self.CI_FILE), ".gitlab-ci.yml must exist"

    def test_valid_yaml(self):
        if not os.path.isfile(self.CI_FILE):
            pytest.skip(".gitlab-ci.yml not yet created")
        with open(self.CI_FILE) as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict), ".gitlab-ci.yml must be valid YAML"

    def test_has_test_stage(self):
        if not os.path.isfile(self.CI_FILE):
            pytest.skip(".gitlab-ci.yml not yet created")
        with open(self.CI_FILE) as f:
            data = yaml.safe_load(f)
        stages = data.get("stages", [])
        assert "test" in stages, ".gitlab-ci.yml: must define a 'test' stage"

    def test_test_job_runs_pytest(self):
        if not os.path.isfile(self.CI_FILE):
            pytest.skip(".gitlab-ci.yml not yet created")
        with open(self.CI_FILE) as f:
            content = f.read()
        assert "pytest" in content, (
            ".gitlab-ci.yml: test job must run pytest"
        )

    def test_uses_python312_image(self):
        if not os.path.isfile(self.CI_FILE):
            pytest.skip(".gitlab-ci.yml not yet created")
        with open(self.CI_FILE) as f:
            content = f.read()
        assert "python:3.12" in content, (
            ".gitlab-ci.yml: must use python:3.12 image"
        )


class TestGitignore:
    """.gitignore must cover sensitive and generated files."""

    GITIGNORE = os.path.join(PROJECT_ROOT, ".gitignore")

    REQUIRED_PATTERNS = [
        "__pycache__",
        ".pytest_cache",
        "03_setup_secrets.sql",
        ".env",
    ]

    def test_file_exists(self):
        assert os.path.isfile(self.GITIGNORE), ".gitignore must exist"

    @pytest.mark.parametrize("pattern", REQUIRED_PATTERNS)
    def test_ignores_pattern(self, pattern):
        if not os.path.isfile(self.GITIGNORE):
            pytest.skip(".gitignore not yet created")
        with open(self.GITIGNORE) as f:
            content = f.read()
        assert pattern in content, (
            f".gitignore: must ignore '{pattern}'"
        )

    def test_does_not_ignore_env_example(self):
        if not os.path.isfile(self.GITIGNORE):
            pytest.skip(".gitignore not yet created")
        with open(self.GITIGNORE) as f:
            content = f.read()
        assert "!.env.example" in content, (
            ".gitignore: must NOT ignore .env.example (use !.env.example)"
        )
