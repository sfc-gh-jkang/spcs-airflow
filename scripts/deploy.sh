#!/usr/bin/env bash
# deploy.sh - Deploy the full Airflow stack to SPCS.
# Runs SQL scripts 01-07 in order, uploads specs and DAGs, then validates.
# Usage: ./scripts/deploy.sh [--connection <name>]
#   --connection: Snowflake connection name (default: snowflake)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_DIR="${PROJECT_DIR}/sql"
SPEC_DIR="${PROJECT_DIR}/specs"
DAGS_DIR="${PROJECT_DIR}/dags"

CONNECTION="snowflake"
while [[ $# -gt 0 ]]; do
    case $1 in
        --connection) CONNECTION="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SNOW_CMD="snow sql --connection ${CONNECTION}"

run_sql_file() {
    local file="$1"
    local desc="$2"
    echo "==> [$(date +%H:%M:%S)] ${desc}: ${file}"
    ${SNOW_CMD} --filename "${file}"
    echo ""
}

echo "============================================="
echo "  Airflow SPCS Deployment"
echo "  Connection: ${CONNECTION}"
echo "============================================="
echo ""

# Pre-flight: ensure secrets have been generated
if [[ ! -f "${SQL_DIR}/03_setup_secrets.sql" ]]; then
    echo "ERROR: sql/03_setup_secrets.sql not found."
    echo "  Generate it first:  bash scripts/generate_secrets.sh"
    exit 1
fi
if grep -q 'CHANGE_ME' "${SQL_DIR}/03_setup_secrets.sql" 2>/dev/null; then
    echo "ERROR: sql/03_setup_secrets.sql still has placeholder values."
    echo "  Generate real secrets:  bash scripts/generate_secrets.sh"
    exit 1
fi

# Step 1: Run setup SQL scripts (01-06)
echo "--- Phase 1: Snowflake Object Setup ---"
run_sql_file "${SQL_DIR}/01_setup_database.sql"    "Database & schema"
run_sql_file "${SQL_DIR}/02_setup_stages.sql"       "Stages"
run_sql_file "${SQL_DIR}/03_setup_secrets.sql"      "Secrets"
run_sql_file "${SQL_DIR}/04_setup_networking.sql"   "Network rules"
run_sql_file "${SQL_DIR}/05_setup_compute_pools.sql" "Compute pools"
run_sql_file "${SQL_DIR}/06_setup_image_repo.sql"   "Image repository"
echo ""

# Step 2: Upload service specs to stage
echo "--- Phase 2: Upload Service Specs ---"
for spec_file in "${SPEC_DIR}"/*.yaml; do
    FILENAME="$(basename "${spec_file}")"
    echo "==> Uploading ${FILENAME} to @AIRFLOW_DB.AIRFLOW_SCHEMA.SERVICE_SPEC"
    ${SNOW_CMD} --query "PUT file://${spec_file} @AIRFLOW_DB.AIRFLOW_SCHEMA.SERVICE_SPEC AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
done
echo ""

# Step 3: Upload DAGs to stage
echo "--- Phase 3: Upload DAGs ---"
for dag_file in "${DAGS_DIR}"/*.py; do
    FILENAME="$(basename "${dag_file}")"
    echo "==> Uploading ${FILENAME} to @AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_DAGS"
    ${SNOW_CMD} --query "PUT file://${dag_file} @AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_DAGS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
done
echo ""

# Step 4: Create services
# NOTE: Docker images must be built and pushed BEFORE this step.
#   Run: ./scripts/build_and_push.sh <REPO_URL>
echo "--- Phase 4: Create Services ---"
echo "==> Checking for required Docker images..."
IMAGE_LIST=$(${SNOW_CMD} --query "CALL SYSTEM\$REGISTRY_LIST_IMAGES('/airflow_db/airflow_schema/airflow_repository');" 2>&1 || true)
MISSING_IMAGES=()
for IMG in airflow airflow-postgres airflow-redis; do
    if ! echo "${IMAGE_LIST}" | grep -qi "${IMG}"; then
        MISSING_IMAGES+=("${IMG}")
    fi
done
if [[ ${#MISSING_IMAGES[@]} -gt 0 ]]; then
    echo ""
    echo "ERROR: Required Docker images not found in repository:"
    for IMG in "${MISSING_IMAGES[@]}"; do
        echo "  - ${IMG}"
    done
    echo ""
    echo "  Build and push images first:"
    echo "    bash scripts/build_and_push.sh <REPO_URL>"
    echo ""
    echo "  Get your REPO_URL with:"
    echo "    snow sql -q \"SHOW IMAGE REPOSITORIES IN SCHEMA AIRFLOW_DB.AIRFLOW_SCHEMA\" --connection ${CONNECTION}"
    exit 1
fi
echo "==> All required images found."
echo ""
run_sql_file "${SQL_DIR}/07_create_services.sql" "Create all 7 services"
echo ""

# Step 5: Validate
echo "--- Phase 5: Validation ---"
run_sql_file "${SQL_DIR}/08_validate.sql" "Validate services"

echo "============================================="
echo "  Deployment complete."
echo "  Run 'snow sql -q \"SHOW ENDPOINTS IN SERVICE AIRFLOW_DB.AIRFLOW_SCHEMA.AF_API_SERVER\" --connection ${CONNECTION}'"
echo "  to get the Airflow UI URL."
echo "============================================="
