#!/usr/bin/env bash
# build_and_push.sh - Build Docker images and push to SPCS image repository.
#
# Usage:
#   ./scripts/build_and_push.sh --connection <name>     # Auto-detect registry URL
#   ./scripts/build_and_push.sh <REPO_URL>              # Explicit registry URL (legacy)
#
# The --connection form queries SHOW IMAGE REPOSITORIES to find the URL automatically.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONNECTION=""
REPO_URL=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --connection) CONNECTION="$2"; shift 2 ;;
        -*) echo "Unknown option: $1"; exit 1 ;;
        *)  REPO_URL="$1"; shift ;;
    esac
done

# Resolve REPO_URL: either from --connection or from positional arg
if [[ -n "${CONNECTION}" ]]; then
    echo "==> Detecting image repository URL via --connection ${CONNECTION}..."
    REPO_OUTPUT=$(snow sql --connection "${CONNECTION}" -q "SHOW IMAGE REPOSITORIES IN SCHEMA AIRFLOW_DB.AIRFLOW_SCHEMA;" 2>&1)

    # Extract the repository_url from the output
    REPO_URL=$(echo "${REPO_OUTPUT}" | grep -i 'airflow_repository' | grep -oE '[a-z0-9._-]+\.registry\.snowflakecomputing\.com/[a-zA-Z0-9_/]+' | head -1)

    if [[ -z "${REPO_URL}" ]]; then
        echo "ERROR: Could not detect image repository URL."
        echo "  Make sure AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_REPOSITORY exists."
        echo "  Run deploy.sh first (it creates the repo in Phase 1), or run:"
        echo "    snow sql --connection ${CONNECTION} -q \"$(cat "${PROJECT_DIR}/sql/06_setup_image_repo.sql")\""
        echo ""
        echo "  Raw output from SHOW IMAGE REPOSITORIES:"
        echo "${REPO_OUTPUT}"
        exit 1
    fi
    echo "==> Found: ${REPO_URL}"
elif [[ -n "${REPO_URL}" ]]; then
    : # User passed explicit URL — use as-is
else
    echo "Usage:"
    echo "  $0 --connection <snow_cli_connection>"
    echo "  $0 <REPO_URL>"
    echo ""
    echo "Examples:"
    echo "  $0 --connection aws_spcs"
    echo "  $0 orgname-acctname.registry.snowflakecomputing.com/airflow_db/airflow_schema/airflow_repository"
    exit 1
fi

# Strip trailing slash
REPO_URL="${REPO_URL%/}"

echo "==> Repository URL: ${REPO_URL}"
echo "==> Project directory: ${PROJECT_DIR}"
echo ""

# Login to SPCS registry
REGISTRY="$(echo "${REPO_URL}" | cut -d'/' -f1)"
echo "==> Logging in to registry: ${REGISTRY}"
if [[ -n "${CONNECTION}" ]]; then
    # Auto-login using snow CLI credentials
    SNOW_TOKEN=$(snow connection generate-token --connection "${CONNECTION}" 2>/dev/null || true)
    if [[ -n "${SNOW_TOKEN}" ]]; then
        echo "${SNOW_TOKEN}" | docker login "${REGISTRY}" --username 0sessiontoken --password-stdin
    else
        echo "    Auto-login failed. Use your Snowflake username and password when prompted."
        docker login "${REGISTRY}"
    fi
else
    echo "    Use your Snowflake username and password when prompted."
    docker login "${REGISTRY}"
fi
echo ""

# Define images to build
declare -A IMAGES=(
    ["airflow:3.1.7"]="images/airflow"
    ["airflow-postgres:17.9"]="images/postgres"
    ["airflow-redis:7.4"]="images/redis"
)

# Build and push each image (linux/amd64 required by SPCS)
for TAG in "${!IMAGES[@]}"; do
    CONTEXT="${PROJECT_DIR}/${IMAGES[$TAG]}"
    FULL_TAG="${REPO_URL}/${TAG}"

    echo "==> Building ${FULL_TAG} from ${CONTEXT}"
    docker build --platform linux/amd64 -t "${FULL_TAG}" "${CONTEXT}"

    echo "==> Pushing ${FULL_TAG}"
    docker push "${FULL_TAG}"
    echo ""
done

echo "==> All images built and pushed successfully."
echo ""
echo "Verify with:"
echo "  CALL SYSTEM\$REGISTRY_LIST_IMAGES('/airflow_db/airflow_schema/airflow_repository');"
