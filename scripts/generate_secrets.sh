#!/usr/bin/env bash
# generate_secrets.sh - Auto-generate secrets for Airflow SPCS deployment.
# Reads sql/03_setup_secrets.sql.template, replaces placeholders with real
# values, and writes to sql/03_setup_secrets.sql (gitignored).
#
# Usage: bash scripts/generate_secrets.sh
#
# Note: Snowflake connection credentials are NOT needed — DAGs use SPCS
# native OAuth tokens automatically (see dags/snowflake_etl_pipeline.py).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE="${PROJECT_DIR}/sql/03_setup_secrets.sql.template"
OUTPUT="${PROJECT_DIR}/sql/03_setup_secrets.sql"

if [[ ! -f "${TEMPLATE}" ]]; then
    echo "ERROR: Template not found: ${TEMPLATE}" >&2
    exit 1
fi

# --- Generate secrets ---
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || {
    echo "ERROR: python3 with 'cryptography' package required for Fernet key generation." >&2
    echo "  Install with: pip3 install cryptography" >&2
    exit 1
})
POSTGRES_PWD=$(python3 -c "import secrets; print(secrets.token_urlsafe(24))")
REDIS_PWD=$(python3 -c "import secrets; print(secrets.token_urlsafe(24))")
JWT_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(64))")

# --- Replace placeholders in template ---
sed \
    -e "s|<CHANGE_ME_FERNET_KEY>|${FERNET_KEY}|g" \
    -e "s|<CHANGE_ME_POSTGRES_PASSWORD>|${POSTGRES_PWD}|g" \
    -e "s|<CHANGE_ME_REDIS_PASSWORD>|${REDIS_PWD}|g" \
    -e "s|<CHANGE_ME_JWT_SECRET>|${JWT_SECRET}|g" \
    "${TEMPLATE}" > "${OUTPUT}"

echo "Secrets generated: ${OUTPUT}"
echo "  AIRFLOW_FERNET_KEY:  generated"
echo "  AIRFLOW_POSTGRES_PWD: generated"
echo "  AIRFLOW_REDIS_PWD:   generated"
echo "  AIRFLOW_JWT_SECRET:  generated (64 bytes, SHA512-safe)"
