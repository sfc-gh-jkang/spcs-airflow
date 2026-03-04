"""Tests for shell script structure and safety.

Validates that all scripts in scripts/ directory:
- Have correct shebang lines
- Use strict error handling (set -e)
- Don't contain hardcoded secrets
- Reference expected files and services
"""

import os
import re

import pytest

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "scripts")

EXPECTED_SCRIPTS = [
    "build_and_push.sh",
    "deploy.sh",
    "generate_secrets.sh",
    "teardown.sh",
]


@pytest.fixture(params=EXPECTED_SCRIPTS)
def script_info(request):
    path = os.path.join(SCRIPTS_DIR, request.param)
    return request.param, path


class TestScriptsExist:
    """All expected scripts must exist."""

    @pytest.mark.parametrize("filename", EXPECTED_SCRIPTS)
    def test_script_exists(self, filename):
        path = os.path.join(SCRIPTS_DIR, filename)
        assert os.path.isfile(path), f"Missing script: scripts/{filename}"


class TestScriptStructure:
    """Scripts must follow shell scripting best practices."""

    def test_has_bash_shebang(self, script_info):
        filename, path = script_info
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            first_line = f.readline().strip()
        assert first_line.startswith("#!/"), (
            f"{filename}: must start with a shebang line"
        )
        assert "bash" in first_line, (
            f"{filename}: shebang must reference bash, got '{first_line}'"
        )

    def test_uses_strict_mode(self, script_info):
        filename, path = script_info
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read()
        assert "set -e" in content, (
            f"{filename}: must use 'set -e' (or set -euo pipefail)"
        )


class TestScriptSecurity:
    """Scripts must not contain hardcoded secrets."""

    SECRET_PATTERNS = [
        r'(?i)password\s*=\s*["\'][^${\s]+["\']',  # password="literal"
        r'(?i)secret\s*=\s*["\'][^${\s]+["\']',
    ]

    @pytest.mark.parametrize("filename", EXPECTED_SCRIPTS)
    def test_no_hardcoded_secrets(self, filename):
        path = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read()
        for pattern in self.SECRET_PATTERNS:
            matches = re.findall(pattern, content)
            # Filter out comments
            real_matches = [
                m for m in matches
                if not any(
                    line.strip().startswith("#") and m in line
                    for line in content.splitlines()
                )
            ]
            assert len(real_matches) == 0, (
                f"{filename}: possible hardcoded secret: {real_matches}"
            )


class TestBuildAndPush:
    """build_and_push.sh must reference all 3 image directories."""

    def test_references_all_images(self):
        path = os.path.join(SCRIPTS_DIR, "build_and_push.sh")
        if not os.path.isfile(path):
            pytest.skip("build_and_push.sh not yet created")
        with open(path) as f:
            content = f.read()
        for image_dir in ["images/airflow", "images/postgres", "images/redis"]:
            assert image_dir in content, (
                f"build_and_push.sh: must reference {image_dir}"
            )


class TestTeardown:
    """teardown.sh must cover all 7 services and have safety features."""

    ALL_SERVICES = [
        "AF_POSTGRES", "AF_REDIS", "AF_API_SERVER",
        "AF_SCHEDULER", "AF_DAG_PROCESSOR", "AF_TRIGGERER", "AF_WORKERS",
    ]

    def test_references_all_services(self):
        path = os.path.join(SCRIPTS_DIR, "teardown.sh")
        if not os.path.isfile(path):
            pytest.skip("teardown.sh not yet created")
        with open(path) as f:
            content = f.read().upper()
        for service in self.ALL_SERVICES:
            assert service in content, (
                f"teardown.sh: must reference service {service}"
            )

    def test_has_full_flag(self):
        path = os.path.join(SCRIPTS_DIR, "teardown.sh")
        if not os.path.isfile(path):
            pytest.skip("teardown.sh not yet created")
        with open(path) as f:
            content = f.read()
        assert "--full" in content, (
            "teardown.sh: must support --full flag for complete teardown"
        )

    def test_has_yes_flag(self):
        path = os.path.join(SCRIPTS_DIR, "teardown.sh")
        if not os.path.isfile(path):
            pytest.skip("teardown.sh not yet created")
        with open(path) as f:
            content = f.read()
        assert "--yes" in content, (
            "teardown.sh: must support --yes flag to skip confirmation"
        )

    def test_has_confirmation_prompt(self):
        path = os.path.join(SCRIPTS_DIR, "teardown.sh")
        if not os.path.isfile(path):
            pytest.skip("teardown.sh not yet created")
        with open(path) as f:
            content = f.read()
        assert "confirm" in content.lower() or "warning" in content.lower(), (
            "teardown.sh: --full must have a confirmation prompt"
        )


class TestDeploy:
    """deploy.sh must run SQL scripts in order and upload files."""

    def test_references_sql_scripts_01_through_07(self):
        path = os.path.join(SCRIPTS_DIR, "deploy.sh")
        if not os.path.isfile(path):
            pytest.skip("deploy.sh not yet created")
        with open(path) as f:
            content = f.read()
        for num in ["01", "02", "03", "04", "05", "06", "07"]:
            assert num in content, (
                f"deploy.sh: must reference SQL script {num}_*.sql"
            )

    def test_uploads_specs_and_dags(self):
        path = os.path.join(SCRIPTS_DIR, "deploy.sh")
        if not os.path.isfile(path):
            pytest.skip("deploy.sh not yet created")
        with open(path) as f:
            content = f.read()
        assert "*.yaml" in content, "deploy.sh: must upload *.yaml specs"
        assert "*.py" in content, "deploy.sh: must upload *.py DAGs"
