#!/usr/bin/env bash
# teardown.sh - Tear down the entire Airflow SPCS stack.
# Drops all services, compute pools, and optionally the database.
# Usage: ./scripts/teardown.sh [--connection <name>] [--full] [--yes]
#   --connection: Snowflake connection name (default: snowflake)
#   --full:       Also drop the database, secrets, and network objects
#   --yes:        Skip confirmation prompt for --full (for scripted use)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONNECTION="snowflake"
FULL_TEARDOWN=false
SKIP_CONFIRM=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --connection) CONNECTION="$2"; shift 2 ;;
        --full) FULL_TEARDOWN=true; shift ;;
        --yes) SKIP_CONFIRM=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SNOW_CMD="snow sql --connection ${CONNECTION}"

run_sql() {
    local desc="$1"
    local query="$2"
    echo "==> ${desc}"
    ${SNOW_CMD} --query "${query}" || true
}

echo "============================================="
echo "  Airflow SPCS Teardown"
echo "  Connection: ${CONNECTION}"
echo "  Full teardown: ${FULL_TEARDOWN}"
echo "============================================="
echo ""

# Phase 1: Drop services (reverse dependency order)
echo "--- Phase 1: Drop Services ---"
run_sql "Drop AF_WORKERS"        "DROP SERVICE IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AF_WORKERS;"
run_sql "Drop AF_TRIGGERER"      "DROP SERVICE IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AF_TRIGGERER;"
run_sql "Drop AF_DAG_PROCESSOR"  "DROP SERVICE IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AF_DAG_PROCESSOR;"
run_sql "Drop AF_SCHEDULER"      "DROP SERVICE IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AF_SCHEDULER;"
run_sql "Drop AF_API_SERVER"     "DROP SERVICE IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AF_API_SERVER;"
run_sql "Drop AF_REDIS"          "DROP SERVICE IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AF_REDIS;"
run_sql "Drop AF_POSTGRES"       "DROP SERVICE IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AF_POSTGRES FORCE;"
echo ""

# Phase 2: Drop compute pools
echo "--- Phase 2: Drop Compute Pools ---"
run_sql "Drop WORKER_POOL"  "DROP COMPUTE POOL IF EXISTS WORKER_POOL;"
run_sql "Drop CORE_POOL"    "DROP COMPUTE POOL IF EXISTS CORE_POOL;"
run_sql "Drop INFRA_POOL"   "DROP COMPUTE POOL IF EXISTS INFRA_POOL;"
echo ""

if [ "${FULL_TEARDOWN}" = true ]; then
    # Safety confirmation for destructive --full teardown
    if [ "${SKIP_CONFIRM}" = false ]; then
        echo "WARNING: --full will DROP DATABASE AIRFLOW_DB on connection '${CONNECTION}'."
        read -rp "Type 'yes' to confirm: " CONFIRM
        if [ "${CONFIRM}" != "yes" ]; then
            echo "Aborted."
            exit 1
        fi
    fi

    # Phase 3: Drop network and integration objects
    echo "--- Phase 3: Drop Network Objects ---"
    run_sql "Drop External Access Integration" "DROP INTEGRATION IF EXISTS AIRFLOW_EXTERNAL_ACCESS;"
    run_sql "Drop Network Rule"                "DROP NETWORK RULE IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_EGRESS_RULE;"
    echo ""

    # Phase 4: Drop secrets
    echo "--- Phase 4: Drop Secrets ---"
    run_sql "Drop AIRFLOW_FERNET_KEY"  "DROP SECRET IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_FERNET_KEY;"
    run_sql "Drop AIRFLOW_POSTGRES_PWD" "DROP SECRET IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_POSTGRES_PWD;"
    run_sql "Drop AIRFLOW_REDIS_PWD"   "DROP SECRET IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_REDIS_PWD;"
    run_sql "Drop AIRFLOW_JWT_SECRET"  "DROP SECRET IF EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_JWT_SECRET;"
    echo ""

    # Phase 5: Drop database (this removes stages, image repo, and schema)
    echo "--- Phase 5: Drop Database ---"
    run_sql "Drop AIRFLOW_SETUP_WH" "DROP WAREHOUSE IF EXISTS AIRFLOW_SETUP_WH;"
    run_sql "Drop AIRFLOW_DB"       "DROP DATABASE IF EXISTS AIRFLOW_DB;"
    echo ""
fi

echo "============================================="
echo "  Teardown complete."
echo "============================================="
