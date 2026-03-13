-- 07_create_services.sql
-- =====================================================================
-- FIRST-TIME DEPLOYMENT ONLY
-- =====================================================================
-- Creates all 7 SPCS services in dependency order.
-- Order: postgres -> redis -> (api-server, scheduler, dag-processor, triggerer) -> workers
-- Idempotent: uses CREATE SERVICE IF NOT EXISTS (skips existing services).
--
-- To UPDATE existing services (new image, config change), use:
--   sql/07b_update_services.sql   (ALTER SERVICE — rolling upgrade)
--   ./scripts/deploy.sh --update  (automated)
--
-- WARNING: Do NOT use CREATE OR REPLACE SERVICE — that generates a NEW
-- ingress URL and breaks bookmarks/integrations.
-- =====================================================================
-- NOTE: Run 01-06 first. Service specs must be uploaded to @SERVICE_SPEC stage.

USE ROLE ACCOUNTADMIN;
USE DATABASE AIRFLOW_DB;
USE SCHEMA AIRFLOW_SCHEMA;

-- 1. PostgreSQL (metadata database) - must start first
CREATE SERVICE IF NOT EXISTS AF_POSTGRES
    IN COMPUTE POOL INFRA_POOL
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_postgres.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- 2. Redis (Celery broker) - must start before Airflow services
CREATE SERVICE IF NOT EXISTS AF_REDIS
    IN COMPUTE POOL INFRA_POOL
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_redis.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- Wait for infra services to be ready before creating Airflow services
-- In practice, use SYSTEM$GET_SERVICE_STATUS to verify readiness

-- 3. API Server (UI + REST API)
CREATE SERVICE IF NOT EXISTS AF_API_SERVER
    IN COMPUTE POOL CORE_POOL
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_api_server.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1
    EXTERNAL_ACCESS_INTEGRATIONS = (AIRFLOW_EXTERNAL_ACCESS);

-- 4. Scheduler
CREATE SERVICE IF NOT EXISTS AF_SCHEDULER
    IN COMPUTE POOL CORE_POOL
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_scheduler.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- 5. DAG Processor (parses DAG files; security boundary)
CREATE SERVICE IF NOT EXISTS AF_DAG_PROCESSOR
    IN COMPUTE POOL CORE_POOL
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_dag_processor.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- 6. Triggerer (handles deferred/async tasks)
CREATE SERVICE IF NOT EXISTS AF_TRIGGERER
    IN COMPUTE POOL CORE_POOL
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_triggerer.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- 7. Workers (Celery workers - auto-scaling)
CREATE SERVICE IF NOT EXISTS AF_WORKERS
    IN COMPUTE POOL WORKER_POOL
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_workers.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 5
    EXTERNAL_ACCESS_INTEGRATIONS = (AIRFLOW_EXTERNAL_ACCESS);
