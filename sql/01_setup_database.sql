-- 01_setup_database.sql
-- Creates the core Snowflake database and schema for Airflow SPCS deployment.
-- Idempotent: safe to re-run.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS AIRFLOW_DB;
CREATE SCHEMA IF NOT EXISTS AIRFLOW_DB.AIRFLOW_SCHEMA;

-- Warehouse for running setup commands (not for Airflow itself)
CREATE WAREHOUSE IF NOT EXISTS AIRFLOW_SETUP_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE DATABASE AIRFLOW_DB;
USE SCHEMA AIRFLOW_SCHEMA;
USE WAREHOUSE AIRFLOW_SETUP_WH;
