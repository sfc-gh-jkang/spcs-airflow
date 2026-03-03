-- 06_setup_image_repo.sql
-- Creates the SPCS image repository for Docker images.
-- Idempotent: uses IF NOT EXISTS.

USE ROLE ACCOUNTADMIN;
USE DATABASE AIRFLOW_DB;
USE SCHEMA AIRFLOW_SCHEMA;

-- Image repository to store airflow, postgres, and redis Docker images
CREATE IMAGE REPOSITORY IF NOT EXISTS AIRFLOW_REPOSITORY;

-- Display repository URL (needed for docker push commands)
SHOW IMAGE REPOSITORIES IN SCHEMA AIRFLOW_DB.AIRFLOW_SCHEMA;
