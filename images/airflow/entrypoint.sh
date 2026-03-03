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

# Run database migration before starting any service.
# Only the first service to run this will actually apply migrations;
# subsequent calls are no-ops if the DB is already current.
echo "Running airflow db migrate..."
airflow db migrate

case "$ROLE" in
    api-server)
        # Set fixed admin password for Simple Auth Manager.
        # Without this, a random password is generated on every restart.
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
