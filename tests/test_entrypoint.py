"""Tests for the Airflow entrypoint.sh role-based dispatch.

Validates:
- Bash shebang and error handling
- All supported AIRFLOW_ROLE values are handled
- Default role is api-server
- Unknown roles produce errors
- api-server runs db migrate and writes auth JSON
- Only api-server runs db migrate (no other roles)
"""

import os
import re

import pytest

ENTRYPOINT = os.path.join(
    os.path.dirname(__file__), "..", "images", "airflow", "entrypoint.sh"
)

EXPECTED_ROLES = [
    "api-server",
    "scheduler",
    "dag-processor",
    "triggerer",
    "worker",
    "db-migrate",
]


@pytest.fixture
def entrypoint_content():
    if not os.path.isfile(ENTRYPOINT):
        pytest.skip("entrypoint.sh not yet created")
    with open(ENTRYPOINT) as f:
        return f.read()


class TestEntrypointStructure:
    """Basic shell script structure."""

    def test_has_bash_shebang(self, entrypoint_content):
        assert entrypoint_content.startswith("#!/bin/bash"), (
            "entrypoint.sh: must start with #!/bin/bash"
        )

    def test_has_set_e(self, entrypoint_content):
        assert "set -e" in entrypoint_content, (
            "entrypoint.sh: must use 'set -e' for fail-fast"
        )

    def test_reads_airflow_role(self, entrypoint_content):
        assert "AIRFLOW_ROLE" in entrypoint_content, (
            "entrypoint.sh: must read AIRFLOW_ROLE env var"
        )

    def test_default_role_is_api_server(self, entrypoint_content):
        assert "AIRFLOW_ROLE:-api-server" in entrypoint_content.replace(" ", ""), (
            "entrypoint.sh: default AIRFLOW_ROLE must be api-server"
        )


class TestEntrypointRoles:
    """All expected roles must be handled in the case statement."""

    @pytest.mark.parametrize("role", EXPECTED_ROLES)
    def test_role_is_handled(self, entrypoint_content, role):
        assert role in entrypoint_content, (
            f"entrypoint.sh: must handle AIRFLOW_ROLE='{role}'"
        )

    def test_unknown_role_exits_with_error(self, entrypoint_content):
        assert "exit 1" in entrypoint_content, (
            "entrypoint.sh: must exit 1 for unknown roles"
        )
        assert "Unknown" in entrypoint_content or "unknown" in entrypoint_content, (
            "entrypoint.sh: must print error for unknown roles"
        )


class TestEntrypointDbMigrate:
    """Only the api-server should run 'airflow db migrate'."""

    def test_api_server_runs_db_migrate(self, entrypoint_content):
        # Find the api-server case block
        api_block = re.search(
            r'api-server\).*?;;', entrypoint_content, re.DOTALL
        )
        assert api_block is not None, (
            "entrypoint.sh: must have api-server case block"
        )
        assert "db migrate" in api_block.group(), (
            "entrypoint.sh: api-server must run 'airflow db migrate'"
        )

    @pytest.mark.parametrize("role", ["scheduler", "dag-processor", "triggerer", "worker"])
    def test_other_roles_skip_db_migrate(self, entrypoint_content, role):
        """Non-api-server roles must NOT run db migrate (race condition)."""
        pattern = rf'{re.escape(role)}\).*?;;'
        block = re.search(pattern, entrypoint_content, re.DOTALL)
        if block is None:
            pytest.skip(f"No case block found for {role}")
        assert "db migrate" not in block.group(), (
            f"entrypoint.sh: role '{role}' must NOT run 'airflow db migrate' "
            "(only api-server should to avoid lock contention)"
        )


class TestEntrypointAuth:
    """api-server must write the Simple Auth Manager JSON file."""

    def test_writes_auth_json(self, entrypoint_content):
        assert "simple_auth_manager_passwords.json" in entrypoint_content, (
            "entrypoint.sh: must write simple_auth_manager_passwords.json.generated"
        )
