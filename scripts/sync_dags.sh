#!/usr/bin/env bash
# sync_dags.sh - Hot-reload DAGs to a running SPCS Airflow deployment.
#
# Uploads DAG files from the local dags/ directory to the AIRFLOW_DAGS stage.
# Includes subdirectories (e.g., dags/utils/) to preserve Python package structure.
# SPCS stage volumes auto-sync to all containers — no service restart needed.
#
# Usage:
#   ./scripts/sync_dags.sh [--connection <name>] [file1.py file2.py ...]
#
# Examples:
#   ./scripts/sync_dags.sh --connection my_connection                  # sync all DAGs + utils
#   ./scripts/sync_dags.sh --connection my_connection my_dag.py        # sync one file
#   ./scripts/sync_dags.sh --connection my_connection foo.py bar.py    # sync specific files
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DAGS_DIR="${PROJECT_DIR}/dags"

SF_DATABASE="AIRFLOW_DB"
SF_SCHEMA="AIRFLOW_SCHEMA"
SF_QUALIFIED="${SF_DATABASE}.${SF_SCHEMA}"

CONNECTION="snowflake"
FILES=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --connection) CONNECTION="$2"; shift 2 ;;
        *) FILES+=("$1"); shift ;;
    esac
done

SNOW_CMD="snow sql --connection ${CONNECTION}"

# Helper: upload a single file to the stage, preserving subdirectory structure.
# Usage: upload_file <absolute_path> <stage_subdir>
#   e.g., upload_file /path/dags/utils/__init__.py utils
upload_file() {
    local filepath="$1"
    local subdir="${2:-}"
    local stage_path="@${SF_QUALIFIED}.AIRFLOW_DAGS"
    if [[ -n "${subdir}" ]]; then
        stage_path="${stage_path}/${subdir}"
    fi
    local display_name="${subdir:+${subdir}/}$(basename "${filepath}")"
    echo "  PUT ${display_name}"
    ${SNOW_CMD} --query "PUT file://${filepath} ${stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;" > /dev/null
}

# If specific files were given, only sync those (top-level only).
if [[ ${#FILES[@]} -gt 0 ]]; then
    echo "==> Syncing ${#FILES[@]} DAG file(s) to @${SF_QUALIFIED}.AIRFLOW_DAGS"
    echo ""
    for FILENAME in "${FILES[@]}"; do
        FILEPATH="${DAGS_DIR}/${FILENAME}"
        if [[ ! -f "${FILEPATH}" ]]; then
            echo "  WARNING: ${FILEPATH} not found, skipping"
            continue
        fi
        upload_file "${FILEPATH}"
    done
else
    # Sync everything: top-level .py files + all subdirectory contents
    FILE_COUNT=0

    # Count files first
    shopt -s nullglob
    for f in "${DAGS_DIR}"/*.py; do
        FILE_COUNT=$((FILE_COUNT + 1))
    done
    # Count subdirectory files (e.g., utils/*.py)
    for subdir in "${DAGS_DIR}"/*/; do
        [[ -d "${subdir}" ]] || continue
        dirname="$(basename "${subdir}")"
        [[ "${dirname}" == "__pycache__" ]] && continue
        for f in "${subdir}"*.py; do
            FILE_COUNT=$((FILE_COUNT + 1))
        done
    done
    shopt -u nullglob

    if [[ ${FILE_COUNT} -eq 0 ]]; then
        echo "No DAG files found in ${DAGS_DIR}"
        exit 1
    fi

    echo "==> Syncing ${FILE_COUNT} file(s) to @${SF_QUALIFIED}.AIRFLOW_DAGS"
    echo ""

    # Upload top-level .py files
    shopt -s nullglob
    for f in "${DAGS_DIR}"/*.py; do
        upload_file "${f}"
    done

    # Upload subdirectory files (preserving directory structure on stage)
    for subdir in "${DAGS_DIR}"/*/; do
        [[ -d "${subdir}" ]] || continue
        dirname="$(basename "${subdir}")"
        [[ "${dirname}" == "__pycache__" ]] && continue
        for f in "${subdir}"*.py; do
            upload_file "${f}" "${dirname}"
        done
    done
    shopt -u nullglob
fi

echo ""
echo "==> Done. DAGs will appear in the Airflow UI within ~60 seconds."
echo "    The dag-processor scans for new/changed files automatically."
echo ""
echo "    Monitor parsing: snow sql --connection ${CONNECTION} -q \\"
echo "      \"CALL SYSTEM\\\$GET_SERVICE_LOGS('${SF_QUALIFIED}.AF_DAG_PROCESSOR', 0, 'dag-processor', 50);\""
