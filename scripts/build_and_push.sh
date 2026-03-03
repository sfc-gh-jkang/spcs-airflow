#!/usr/bin/env bash
# build_and_push.sh - Build Docker images and push to SPCS image repository.
# Usage: ./scripts/build_and_push.sh <REPO_URL>
#   REPO_URL: SPCS image repository URL (from SHOW IMAGE REPOSITORIES output)
#   Example:  ./scripts/build_and_push.sh orgname-acctname.registry.snowflakecomputing.com/airflow_db/airflow_schema/airflow_repository
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

REPO_URL="${1:?Usage: $0 <REPO_URL>}"

# Strip trailing slash
REPO_URL="${REPO_URL%/}"

echo "==> Repository URL: ${REPO_URL}"
echo "==> Project directory: ${PROJECT_DIR}"
echo ""

# Login to SPCS registry (prompts for password — use your Snowflake credentials)
REGISTRY="$(echo "${REPO_URL}" | cut -d'/' -f1)"
echo "==> Logging in to registry: ${REGISTRY}"
echo "    Use your Snowflake username and password when prompted."
docker login "${REGISTRY}"
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
