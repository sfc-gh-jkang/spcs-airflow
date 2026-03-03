"""Tests for cross-service DNS connectivity patterns.

Validates that service specs reference other services using correct
SPCS internal DNS patterns. SPCS converts underscores in service names
to hyphens for DNS resolution:
  AF_POSTGRES -> af-postgres
  AF_REDIS -> af-redis
  AF_API_SERVER -> af-api-server

These are structural tests (not live connectivity tests).
"""

import os
import yaml
import pytest

SPECS_DIR = os.path.join(os.path.dirname(__file__), "..", "specs")

# Services and what they need to connect to
# NOTE: SPCS DNS names use hyphens, not underscores
CONNECTIVITY_MATRIX = {
    "af_api_server.yaml": {
        "must_reference": ["af-postgres", "af-redis", "af-api-server"],
        "ports": {"af-postgres": "5432", "af-redis": "6379", "af-api-server": "8080"},
    },
    "af_scheduler.yaml": {
        "must_reference": ["af-postgres", "af-redis", "af-api-server"],
        "ports": {"af-postgres": "5432", "af-redis": "6379", "af-api-server": "8080"},
    },
    "af_dag_processor.yaml": {
        "must_reference": ["af-postgres", "af-redis", "af-api-server"],
        "ports": {"af-postgres": "5432", "af-redis": "6379", "af-api-server": "8080"},
    },
    "af_triggerer.yaml": {
        "must_reference": ["af-postgres", "af-redis", "af-api-server"],
        "ports": {"af-postgres": "5432", "af-redis": "6379", "af-api-server": "8080"},
    },
    "af_workers.yaml": {
        "must_reference": ["af-postgres", "af-redis", "af-api-server"],
        "ports": {"af-postgres": "5432", "af-redis": "6379", "af-api-server": "8080"},
    },
}


class TestServiceConnectivity:
    """Services must reference their dependencies via DNS or env vars."""

    @pytest.mark.parametrize(
        "filename,deps", list(CONNECTIVITY_MATRIX.items())
    )
    def test_references_dependencies(self, filename, deps):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read()

        for dep_service in deps["must_reference"]:
            # Check that the dependency service name appears in the spec
            # (could be in env vars, connection strings, or command args)
            assert dep_service in content, (
                f"{filename}: must reference dependency '{dep_service}' "
                f"(for cross-service communication)"
            )

    @pytest.mark.parametrize(
        "filename,deps", list(CONNECTIVITY_MATRIX.items())
    )
    def test_references_correct_ports(self, filename, deps):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read()

        for dep_service, port in deps["ports"].items():
            assert port in content, (
                f"{filename}: must reference port {port} for {dep_service}"
            )


class TestNoLocalhostReferences:
    """Cross-service specs must NOT use localhost for dependencies.

    Only services within the SAME spec can use localhost.
    Since we have 7 separate services, all cross-service communication
    must use DNS names.
    """

    @pytest.mark.parametrize("filename", list(CONNECTIVITY_MATRIX.keys()))
    def test_no_localhost_for_external_deps(self, filename):
        path = os.path.join(SPECS_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read()

        # Check that postgres/redis connections don't use localhost
        # (they should use DNS service names)
        lines = content.splitlines()
        for i, line in enumerate(lines):
            if "localhost:5432" in line or "127.0.0.1:5432" in line:
                pytest.fail(
                    f"{filename}:{i+1}: uses localhost for postgres; "
                    "must use 'af-postgres' DNS name"
                )
            if "localhost:6379" in line or "127.0.0.1:6379" in line:
                pytest.fail(
                    f"{filename}:{i+1}: uses localhost for redis; "
                    "must use 'af-redis' DNS name"
                )
