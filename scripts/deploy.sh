#!/usr/bin/env bash
# deploy.sh - Deploy the full Airflow stack to SPCS.
#
# Usage:
#   ./scripts/deploy.sh [--connection <name>]            # First-time deploy (CREATE)
#   ./scripts/deploy.sh [--connection <name>] --update    # Update existing stack (ALTER)
#
# Options:
#   --connection: Snowflake connection name (default: snowflake)
#   --update:     Update existing services via ALTER SERVICE (preserves URLs)
#
# First-time deploy: runs SQL 01-07, uploads specs/DAGs, validates.
# Update deploy:     uploads specs/DAGs, runs ALTER SERVICE on all 7 services.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_DIR="${PROJECT_DIR}/sql"
SPEC_DIR="${PROJECT_DIR}/specs"
DAGS_DIR="${PROJECT_DIR}/dags"

CONNECTION="snowflake"
UPDATE_MODE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --connection) CONNECTION="$2"; shift 2 ;;
        --update) UPDATE_MODE=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SNOW_CMD="snow sql --connection ${CONNECTION}"

# Snowflake object names (single source of truth)
SF_DATABASE="AIRFLOW_DB"
SF_SCHEMA="AIRFLOW_SCHEMA"
SF_REPO="AIRFLOW_REPOSITORY"
SF_QUALIFIED="${SF_DATABASE}.${SF_SCHEMA}"

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
echo "  Mode:       $(if ${UPDATE_MODE}; then echo 'UPDATE (ALTER SERVICE)'; else echo 'CREATE (first-time)'; fi)"
echo "============================================="
echo ""

# Helper: check if Airflow services already exist
services_exist() {
    local result
    result=$(${SNOW_CMD} --query "SHOW SERVICES LIKE 'AF_%' IN SCHEMA ${SF_QUALIFIED};" --format json 2>/dev/null || echo "[]")
    local count
    count=$(echo "${result}" | python3 -c "import sys,json; data=json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "0")
    [[ "${count}" -gt 0 ]]
}

if ${UPDATE_MODE}; then
    # --update mode: verify services exist before attempting ALTER
    if ! services_exist; then
        echo "ERROR: No existing Airflow services found in ${SF_QUALIFIED}."
        echo "  Run a first-time deploy first (without --update):"
        echo "    ./scripts/deploy.sh --connection ${CONNECTION}"
        exit 1
    fi
else
    # First-time mode: warn if services already exist
    if services_exist; then
        echo "WARNING: Airflow services already exist in ${SF_QUALIFIED}."
        echo "  Running CREATE again is safe (IF NOT EXISTS), but to update"
        echo "  services without changing the ingress URL, use --update:"
        echo ""
        echo "    ./scripts/deploy.sh --connection ${CONNECTION} --update"
        echo ""
        read -rp "  Continue with first-time deploy anyway? [y/N]: " CONFIRM
        if [[ "${CONFIRM}" != "y" && "${CONFIRM}" != "Y" ]]; then
            echo "Aborted. Use --update to update existing services."
            exit 0
        fi
        echo ""
    fi
fi

# Pre-flight: ensure secrets have been generated (skip in update mode)
if ! ${UPDATE_MODE}; then
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
fi

# Step 1: Run setup SQL scripts (01-06) — first-time only
if ${UPDATE_MODE}; then
    echo "--- Phase 1: Skipped (--update mode, infra already exists) ---"
    echo ""
else
    echo "--- Phase 1: Snowflake Object Setup ---"
    run_sql_file "${SQL_DIR}/01_setup_database.sql"    "Database & schema"
    run_sql_file "${SQL_DIR}/02_setup_stages.sql"       "Stages"
    run_sql_file "${SQL_DIR}/03_setup_secrets.sql"      "Secrets"
    run_sql_file "${SQL_DIR}/04_setup_networking.sql"   "Network rules"
    run_sql_file "${SQL_DIR}/05_setup_compute_pools.sql" "Compute pools"
    run_sql_file "${SQL_DIR}/06_setup_image_repo.sql"   "Image repository"
    echo ""
fi

# Step 2: Upload service specs to stage
echo "--- Phase 2: Upload Service Specs ---"
shopt -s nullglob
for spec_file in "${SPEC_DIR}"/*.yaml; do
    FILENAME="$(basename "${spec_file}")"
    echo "==> Uploading ${FILENAME} to @${SF_QUALIFIED}.SERVICE_SPEC"
    ${SNOW_CMD} --query "PUT file://${spec_file} @${SF_QUALIFIED}.SERVICE_SPEC AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
done
shopt -u nullglob
echo ""

# Step 3: Upload DAGs to stage
echo "--- Phase 3: Upload DAGs ---"
shopt -s nullglob
for dag_file in "${DAGS_DIR}"/*.py; do
    FILENAME="$(basename "${dag_file}")"
    echo "==> Uploading ${FILENAME} to @${SF_QUALIFIED}.AIRFLOW_DAGS"
    ${SNOW_CMD} --query "PUT file://${dag_file} @${SF_QUALIFIED}.AIRFLOW_DAGS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
done
# Upload subdirectories (e.g., dags/utils/) preserving directory structure
for subdir in "${DAGS_DIR}"/*/; do
    [[ -d "${subdir}" ]] || continue
    dirname="$(basename "${subdir}")"
    [[ "${dirname}" == "__pycache__" ]] && continue
    for f in "${subdir}"*.py; do
        FILENAME="$(basename "${f}")"
        echo "==> Uploading ${dirname}/${FILENAME} to @${SF_QUALIFIED}.AIRFLOW_DAGS/${dirname}"
        ${SNOW_CMD} --query "PUT file://${f} @${SF_QUALIFIED}.AIRFLOW_DAGS/${dirname} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
    done
done
shopt -u nullglob
echo ""

# Step 4: Create or update services
# NOTE: Docker images must be built and pushed BEFORE this step.
#   Run: ./scripts/build_and_push.sh --connection <name>
if ${UPDATE_MODE}; then
    echo "--- Phase 4: Update Services (ALTER SERVICE) ---"
    echo "==> Rolling upgrade — ingress URL will NOT change."
    echo ""
    run_sql_file "${SQL_DIR}/07b_update_services.sql" "ALTER all 7 services"
else
    echo "--- Phase 4: Create Services ---"
    echo "==> Checking for required Docker images..."
    IMAGE_LIST=$(${SNOW_CMD} --query "CALL SYSTEM\$REGISTRY_LIST_IMAGES('/${SF_DATABASE}/${SF_SCHEMA}/${SF_REPO}');" 2>&1 || true)

    # SYSTEM$REGISTRY_LIST_IMAGES can return 401 on some cloud providers (known GCP issue).
    # If the call failed, warn and proceed — service creation will fail fast if images are missing.
    if echo "${IMAGE_LIST}" | grep -qi "error\|unauthorized\|failed"; then
        echo "==> WARNING: Could not query image registry (this is normal on some cloud providers)."
        echo "    Proceeding to service creation. If images are missing, services will fail to start."
        echo ""
    else
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
            echo "    bash scripts/build_and_push.sh --connection ${CONNECTION}"
            exit 1
        fi
        echo "==> All required images found."
        echo ""
    fi
    run_sql_file "${SQL_DIR}/07_create_services.sql" "Create all 7 services"
fi
echo ""

# Step 5: Wait for services to stabilize, then validate
echo "--- Phase 5: Validation ---"

ALL_SERVICES="AF_POSTGRES AF_REDIS AF_API_SERVER AF_SCHEDULER AF_DAG_PROCESSOR AF_TRIGGERER AF_WORKERS"
MAX_WAIT=180   # seconds
POLL_INTERVAL=10

wait_for_services_ready() {
    local elapsed=0
    while [[ ${elapsed} -lt ${MAX_WAIT} ]]; do
        local not_ready=()
        for svc in ${ALL_SERVICES}; do
            local status
            status=$(${SNOW_CMD} --query "SELECT SYSTEM\$GET_SERVICE_STATUS('${SF_QUALIFIED}.${svc}');" --format json 2>/dev/null \
                | python3 -c "import sys,json; data=json.load(sys.stdin); print(json.loads(data[0][list(data[0].keys())[0]])[0]['status'])" 2>/dev/null || echo "UNKNOWN")
            if [[ "${status}" != "READY" ]]; then
                not_ready+=("${svc}:${status}")
            fi
        done
        if [[ ${#not_ready[@]} -eq 0 ]]; then
            echo "==> All 7 services are READY."
            return 0
        fi
        echo "==> [$(date +%H:%M:%S)] Waiting for services: ${not_ready[*]} (${elapsed}s/${MAX_WAIT}s)"
        sleep ${POLL_INTERVAL}
        elapsed=$((elapsed + POLL_INTERVAL))
    done
    echo "WARNING: Some services not READY after ${MAX_WAIT}s. Running validation anyway."
    return 0
}

if ${UPDATE_MODE}; then
    echo "==> Waiting for rolling restart to complete..."
    wait_for_services_ready
    echo ""
fi

run_sql_file "${SQL_DIR}/08_validate.sql" "Validate services"

# Show endpoint URL and login instructions
echo ""
ENDPOINT_URL=$(${SNOW_CMD} --query "SHOW ENDPOINTS IN SERVICE ${SF_QUALIFIED}.AF_API_SERVER;" --format json 2>/dev/null \
    | python3 -c "import sys,json; data=json.load(sys.stdin); print(data[0]['ingress_url'])" 2>/dev/null || true)

echo "============================================="
echo "  Deployment complete!"
if [[ -n "${ENDPOINT_URL}" ]]; then
echo ""
echo "  Airflow UI: https://${ENDPOINT_URL}"
echo ""
echo "  Login:"
echo "    1. Authenticate via Snowflake SSO (automatic on first visit)"
echo "    2. Airflow credentials: admin / admin"
fi
echo ""
echo "  Useful commands:"
echo "    snow sql -q \"SHOW ENDPOINTS IN SERVICE ${SF_QUALIFIED}.AF_API_SERVER\" --connection ${CONNECTION}"
echo "    snow sql -q \"SELECT SYSTEM\\\$GET_SERVICE_STATUS('${SF_QUALIFIED}.AF_API_SERVER')\" --connection ${CONNECTION}"
echo "============================================="
