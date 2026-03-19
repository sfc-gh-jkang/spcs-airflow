-- 07b_update_services.sql
-- Updates all 7 SPCS services via ALTER SERVICE (rolling upgrade).
-- This preserves the ingress URL. Use this for ALL updates after initial deployment.
-- Order: postgres -> redis -> (api-server, scheduler, dag-processor, triggerer) -> workers
--
-- WARNING: Do NOT use CREATE OR REPLACE SERVICE — that drops and recreates the
-- service, which generates a NEW ingress URL and breaks bookmarks/integrations.
--
-- Prerequisites:
--   - Services must already exist (created via 07_create_services.sql)
--   - Updated spec files must be uploaded to @SERVICE_SPEC stage
--   - Updated Docker images must be pushed to the image repository

USE ROLE ACCOUNTADMIN;
USE DATABASE AIRFLOW_DB;
USE SCHEMA AIRFLOW_SCHEMA;

-- 1. PostgreSQL (metadata database)
ALTER SERVICE AF_POSTGRES
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_postgres.yaml';

-- 2. Redis (Celery broker)
ALTER SERVICE AF_REDIS
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_redis.yaml';

-- 3. API Server (UI + REST API)
-- NOTE: EXTERNAL_ACCESS_INTEGRATIONS is preserved from CREATE SERVICE.
-- ALTER SERVICE does not support re-specifying it with FROM @STAGE syntax.
ALTER SERVICE AF_API_SERVER
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_api_server.yaml';

-- 4. Scheduler
ALTER SERVICE AF_SCHEDULER
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_scheduler.yaml';

-- 5. DAG Processor
ALTER SERVICE AF_DAG_PROCESSOR
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_dag_processor.yaml';

-- 6. Triggerer
ALTER SERVICE AF_TRIGGERER
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_triggerer.yaml';

-- 7. Workers
-- NOTE: EXTERNAL_ACCESS_INTEGRATIONS is preserved from CREATE SERVICE.
ALTER SERVICE AF_WORKERS
    FROM @SERVICE_SPEC
    SPECIFICATION_FILE = 'af_workers.yaml';
