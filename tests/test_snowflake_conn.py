"""Tests for the shared Snowflake connection helper.

Validates that dags/utils/snowflake_conn.py:
- Detects SPCS vs local environment correctly
- Uses OAuth token when on SPCS
- Uses env var credentials when local
- Raises RuntimeError when required local env vars are missing
- Passes optional kwargs (database, schema, warehouse) through
"""

import os
import sys
import types
from unittest import mock

import pytest

# Add dags/ to sys.path so we can import the helper directly
DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")
sys.path.insert(0, DAGS_DIR)

from utils.snowflake_conn import SPCS_TOKEN_PATH, get_snowflake_connection, is_running_on_spcs


class TestSpcsDetection:
    """is_running_on_spcs() must detect SPCS vs local environment."""

    def test_returns_false_locally(self):
        """On a dev machine, /snowflake/session/token does not exist."""
        assert is_running_on_spcs() is False

    def test_returns_true_when_token_exists(self, tmp_path):
        """When the token file exists, we're on SPCS."""
        token_file = tmp_path / "token"
        token_file.write_text("mock-token")
        with mock.patch("utils.snowflake_conn.SPCS_TOKEN_PATH", str(token_file)):
            # Re-import to pick up the patched constant — or just call with patched os.path.isfile
            with mock.patch("os.path.isfile", return_value=True):
                assert is_running_on_spcs() is True

    def test_token_path_is_correct(self):
        assert SPCS_TOKEN_PATH == "/snowflake/session/token"


class TestSpcsConnection:
    """On SPCS, get_snowflake_connection() uses OAuth token."""

    def test_uses_oauth_on_spcs(self, tmp_path):
        """When SPCS token exists, connects with authenticator=oauth."""
        token_file = tmp_path / "token"
        token_file.write_text("test-oauth-token")

        mock_connector = mock.MagicMock()
        mock_sf_connector = types.ModuleType("snowflake.connector")
        mock_sf_connector.connect = mock_connector
        mock_sf = types.ModuleType("snowflake")
        mock_sf.connector = mock_sf_connector

        with mock.patch("os.path.isfile", return_value=True), \
             mock.patch("utils.snowflake_conn.SPCS_TOKEN_PATH", str(token_file)), \
             mock.patch.dict("sys.modules", {"snowflake": mock_sf, "snowflake.connector": mock_sf_connector}), \
             mock.patch.dict(os.environ, {"SNOWFLAKE_HOST": "test-host", "SNOWFLAKE_ACCOUNT": "test-account"}, clear=False):
            get_snowflake_connection()

        mock_connector.assert_called_once()
        call_kwargs = mock_connector.call_args[1]
        assert call_kwargs["authenticator"] == "oauth"
        assert call_kwargs["token"] == "test-oauth-token"
        assert call_kwargs["host"] == "test-host"
        assert call_kwargs["account"] == "test-account"


class TestLocalConnection:
    """Locally, get_snowflake_connection() uses env var credentials."""

    def test_uses_env_vars_locally(self):
        """When no SPCS token, connects with account/user/password from env."""
        mock_connector = mock.MagicMock()
        mock_sf_connector = types.ModuleType("snowflake.connector")
        mock_sf_connector.connect = mock_connector
        mock_sf = types.ModuleType("snowflake")
        mock_sf.connector = mock_sf_connector

        env = {
            "SNOWFLAKE_ACCOUNT": "my-account",
            "SNOWFLAKE_USER": "my-user",
            "SNOWFLAKE_PASSWORD": "my-password",
        }

        with mock.patch("os.path.isfile", return_value=False), \
             mock.patch.dict("sys.modules", {"snowflake": mock_sf, "snowflake.connector": mock_sf_connector}), \
             mock.patch.dict(os.environ, env, clear=False):
            get_snowflake_connection()

        mock_connector.assert_called_once()
        call_kwargs = mock_connector.call_args[1]
        assert call_kwargs["account"] == "my-account"
        assert call_kwargs["user"] == "my-user"
        assert call_kwargs["password"] == "my-password"
        assert "authenticator" not in call_kwargs
        assert "token" not in call_kwargs

    def test_raises_without_required_env_vars(self):
        """Missing SNOWFLAKE_ACCOUNT or SNOWFLAKE_USER raises RuntimeError."""
        with mock.patch("os.path.isfile", return_value=False), \
             mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(RuntimeError, match="SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER"):
                get_snowflake_connection()

    def test_passes_optional_env_vars(self):
        """Optional env vars (database, schema, warehouse, role) are forwarded."""
        mock_connector = mock.MagicMock()
        mock_sf_connector = types.ModuleType("snowflake.connector")
        mock_sf_connector.connect = mock_connector
        mock_sf = types.ModuleType("snowflake")
        mock_sf.connector = mock_sf_connector

        env = {
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "usr",
            "SNOWFLAKE_PASSWORD": "pwd",
            "SNOWFLAKE_DATABASE": "MY_DB",
            "SNOWFLAKE_SCHEMA": "MY_SCHEMA",
            "SNOWFLAKE_WAREHOUSE": "MY_WH",
            "SNOWFLAKE_ROLE": "MY_ROLE",
        }

        with mock.patch("os.path.isfile", return_value=False), \
             mock.patch.dict("sys.modules", {"snowflake": mock_sf, "snowflake.connector": mock_sf_connector}), \
             mock.patch.dict(os.environ, env, clear=False):
            get_snowflake_connection()

        call_kwargs = mock_connector.call_args[1]
        assert call_kwargs["database"] == "MY_DB"
        assert call_kwargs["schema"] == "MY_SCHEMA"
        assert call_kwargs["warehouse"] == "MY_WH"
        assert call_kwargs["role"] == "MY_ROLE"


class TestKwargsOverride:
    """Caller kwargs must override env var defaults."""

    def test_kwargs_override_env(self):
        mock_connector = mock.MagicMock()
        mock_sf_connector = types.ModuleType("snowflake.connector")
        mock_sf_connector.connect = mock_connector
        mock_sf = types.ModuleType("snowflake")
        mock_sf.connector = mock_sf_connector

        env = {
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "usr",
            "SNOWFLAKE_PASSWORD": "pwd",
            "SNOWFLAKE_DATABASE": "ENV_DB",
        }

        with mock.patch("os.path.isfile", return_value=False), \
             mock.patch.dict("sys.modules", {"snowflake": mock_sf, "snowflake.connector": mock_sf_connector}), \
             mock.patch.dict(os.environ, env, clear=False):
            get_snowflake_connection(database="OVERRIDE_DB")

        call_kwargs = mock_connector.call_args[1]
        assert call_kwargs["database"] == "OVERRIDE_DB"


class TestHelperModuleStructure:
    """The helper module must exist and be importable."""

    def test_utils_init_exists(self):
        init_path = os.path.join(DAGS_DIR, "utils", "__init__.py")
        assert os.path.isfile(init_path)

    def test_snowflake_conn_exists(self):
        module_path = os.path.join(DAGS_DIR, "utils", "snowflake_conn.py")
        assert os.path.isfile(module_path)

    def test_exports_expected_functions(self):
        from utils.snowflake_conn import get_snowflake_connection, is_running_on_spcs, run_sql
        assert callable(get_snowflake_connection)
        assert callable(is_running_on_spcs)
        assert callable(run_sql)
