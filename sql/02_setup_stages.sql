-- 02_setup_stages.sql
-- Creates Snowflake stages for service specs, DAG files, and task logs.
-- All stages use SNOWFLAKE_SSE encryption as required by SPCS.
-- Idempotent: safe to re-run.

USE ROLE ACCOUNTADMIN;
USE DATABASE AIRFLOW_DB;
USE SCHEMA AIRFLOW_SCHEMA;

-- Stage for SPCS service specification YAML files
CREATE STAGE IF NOT EXISTS SERVICE_SPEC
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Stage for Airflow DAG files (synced from git or uploaded manually)
CREATE STAGE IF NOT EXISTS AIRFLOW_DAGS
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- Stage for Airflow task logs (written by workers, read by UI)
CREATE STAGE IF NOT EXISTS AIRFLOW_LOGS
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);
