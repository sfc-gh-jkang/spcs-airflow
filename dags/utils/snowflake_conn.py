"""Snowflake connection helper with SPCS/local auto-detection.

Inside SPCS containers, Snowflake provides:
- /snowflake/session/token — OAuth token (auto-refreshed)
- SNOWFLAKE_ACCOUNT env var
- SNOWFLAKE_HOST env var

For local development, set these environment variables:
- SNOWFLAKE_ACCOUNT (required)
- SNOWFLAKE_USER (required)
- SNOWFLAKE_PASSWORD (required, unless using key-pair auth)
- SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE (optional)

Detection: if /snowflake/session/token exists -> SPCS OAuth mode,
otherwise -> local mode using env vars.
"""

import logging
import os

logger = logging.getLogger(__name__)

SPCS_TOKEN_PATH = "/snowflake/session/token"


def is_running_on_spcs() -> bool:
    """Return True if running inside an SPCS container."""
    return os.path.isfile(SPCS_TOKEN_PATH)


def get_snowflake_connection(**kwargs):
    """Create a Snowflake connection, auto-detecting SPCS vs local.

    Any keyword arguments are forwarded to snowflake.connector.connect()
    and override the auto-detected defaults (e.g., database, schema, warehouse).
    """
    import snowflake.connector

    if is_running_on_spcs():
        logger.debug("SPCS detected -- using OAuth token from %s", SPCS_TOKEN_PATH)
        with open(SPCS_TOKEN_PATH, "r") as f:
            token = f.read()

        defaults = {
            "host": os.getenv("SNOWFLAKE_HOST"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "token": token,
            "authenticator": "oauth",
        }
    else:
        logger.debug("Local environment -- using env var credentials")
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
        if not account or not user:
            raise RuntimeError(
                "Local mode requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER env vars. "
                "See .env.example for the full list."
            )

        defaults = {
            "account": account,
            "user": user,
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
        }

    # Optional overrides from env
    for env_key, conn_key in [
        ("SNOWFLAKE_DATABASE", "database"),
        ("SNOWFLAKE_SCHEMA", "schema"),
        ("SNOWFLAKE_WAREHOUSE", "warehouse"),
        ("SNOWFLAKE_ROLE", "role"),
    ]:
        val = os.getenv(env_key)
        if val:
            defaults[conn_key] = val

    # Caller kwargs win over defaults
    defaults.update(kwargs)
    return snowflake.connector.connect(**defaults)


def run_sql(sql: str, fetch: bool = False, **conn_kwargs):
    """Execute SQL and optionally return results.

    Uses get_snowflake_connection() for auto-detected auth.
    Extra keyword arguments are forwarded to get_snowflake_connection().
    """
    conn = get_snowflake_connection(**conn_kwargs)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if fetch:
            return cur.fetchall()
    finally:
        conn.close()
