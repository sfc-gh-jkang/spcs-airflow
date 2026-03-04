#!/bin/bash
set -e

# Role-based entrypoint for Airflow 3.x containers.
# The AIRFLOW_ROLE environment variable determines which process to start.
# Supported roles: api-server, scheduler, dag-processor, triggerer, worker
#
# Usage in SPCS service spec:
#   env:
#     AIRFLOW_ROLE: "scheduler"

ROLE="${AIRFLOW_ROLE:-api-server}"

case "$ROLE" in
    api-server)
        # Only the api-server runs db migrate to avoid lock contention from
        # multiple containers migrating simultaneously on startup.
        echo "Running airflow db migrate..."
        airflow db migrate

        # Set fixed admin password for Simple Auth Manager.
        # This is the actual auth source — the JSON file controls login credentials.
        echo '{"admin": "admin"}' > /opt/airflow/simple_auth_manager_passwords.json.generated
        echo "Starting Airflow API Server..."
        exec airflow api-server --port 8080
        ;;
    scheduler)
        echo "Starting Airflow Scheduler..."
        exec airflow scheduler
        ;;
    dag-processor)
        echo "Starting Airflow DAG Processor..."
        exec airflow dag-processor
        ;;
    triggerer)
        echo "Starting Airflow Triggerer..."
        exec airflow triggerer
        ;;
    worker)
        echo "Starting Airflow Celery Worker..."
        exec airflow celery worker
        ;;
    db-migrate)
        echo "Running Airflow DB Migrate..."
        airflow db migrate
        echo "DB migration complete."
        ;;
    *)
        echo "Unknown AIRFLOW_ROLE: $ROLE"
        echo "Supported roles: api-server, scheduler, dag-processor, triggerer, worker, db-migrate"
        exit 1
        ;;
esac
