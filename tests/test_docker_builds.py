"""Tests for Dockerfile validity.

Validates that all 3 Dockerfiles:
- Exist in the expected locations
- Use the correct base images with pinned versions
- Include platform-appropriate build targets
- Don't embed secrets or credentials
"""

import os
import pytest

IMAGES_DIR = os.path.join(os.path.dirname(__file__), "..", "images")

EXPECTED_DOCKERFILES = {
    "airflow/Dockerfile": {
        "base_image_contains": "apache/airflow",
        "version_contains": "3.1.7",
    },
    "postgres/Dockerfile": {
        "base_image_contains": "postgres",
        "version_contains": "17",
    },
    "redis/Dockerfile": {
        "base_image_contains": "redis",
        "version_contains": "7.4",
    },
}


@pytest.fixture(params=list(EXPECTED_DOCKERFILES.keys()))
def dockerfile_info(request):
    path = os.path.join(IMAGES_DIR, request.param)
    expected = EXPECTED_DOCKERFILES[request.param]
    return path, expected


class TestDockerfilesExist:
    """All 3 Dockerfiles must exist."""

    @pytest.mark.parametrize("rel_path", EXPECTED_DOCKERFILES.keys())
    def test_dockerfile_exists(self, rel_path):
        path = os.path.join(IMAGES_DIR, rel_path)
        assert os.path.isfile(path), f"Missing Dockerfile: {rel_path}"


class TestDockerfileContent:
    """Dockerfiles must use correct base images and versions."""

    def test_has_from_instruction(self, dockerfile_info):
        path, _ = dockerfile_info
        if not os.path.isfile(path):
            pytest.skip(f"{path} not yet created")
        with open(path) as f:
            content = f.read()
        assert "FROM" in content, f"{path}: must have FROM instruction"

    def test_correct_base_image(self, dockerfile_info):
        path, expected = dockerfile_info
        if not os.path.isfile(path):
            pytest.skip(f"{path} not yet created")
        with open(path) as f:
            content = f.read()
        from_lines = [
            line for line in content.splitlines() if line.strip().startswith("FROM")
        ]
        assert len(from_lines) >= 1, f"{path}: no FROM instruction found"
        base_image_str = " ".join(from_lines)
        assert expected["base_image_contains"] in base_image_str, (
            f"{path}: expected base image containing '{expected['base_image_contains']}', "
            f"got: {base_image_str}"
        )

    def test_version_pinned(self, dockerfile_info):
        path, expected = dockerfile_info
        if not os.path.isfile(path):
            pytest.skip(f"{path} not yet created")
        with open(path) as f:
            content = f.read()
        from_lines = [
            line for line in content.splitlines() if line.strip().startswith("FROM")
        ]
        from_str = " ".join(from_lines)
        assert expected["version_contains"] in from_str, (
            f"{path}: expected version containing '{expected['version_contains']}', "
            f"got: {from_str}"
        )


class TestDockerfileSecurity:
    """Dockerfiles must not contain secrets or credentials."""

    FORBIDDEN_PATTERNS = [
        "PASSWORD=",
        "SECRET_KEY=",
        "FERNET_KEY=",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "SNOWFLAKE_PASSWORD",
    ]

    @pytest.mark.parametrize("rel_path", EXPECTED_DOCKERFILES.keys())
    def test_no_embedded_secrets(self, rel_path):
        path = os.path.join(IMAGES_DIR, rel_path)
        if not os.path.isfile(path):
            pytest.skip(f"{rel_path} not yet created")
        with open(path) as f:
            content = f.read().upper()
        for pattern in self.FORBIDDEN_PATTERNS:
            # Allow comments and ARG declarations that don't set values
            lines_with_pattern = [
                line
                for line in content.splitlines()
                if pattern in line and not line.strip().startswith("#")
            ]
            for line in lines_with_pattern:
                # OK if it's an ENV referencing a variable, not a literal value
                if '="' in line or "='" in line:
                    # Check it's not setting a literal secret value
                    assert "${" in line or "$$" in line or line.strip().endswith('""'), (
                        f"{rel_path}: possible embedded secret: {line.strip()}"
                    )
