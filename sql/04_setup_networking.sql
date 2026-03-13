-- 04_setup_networking.sql
-- Creates External Access Integration and network rules for outbound access.
-- Required for: outbound HTTPS (PyPI packages, Docker registry, GitHub).
-- Idempotent: uses CREATE OR REPLACE.

USE ROLE ACCOUNTADMIN;
USE DATABASE AIRFLOW_DB;
USE SCHEMA AIRFLOW_SCHEMA;

-- Network rule allowing outbound HTTPS to common package registries
CREATE OR REPLACE NETWORK RULE AIRFLOW_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'pypi.org',
        'files.pythonhosted.org',
        'github.com',
        'raw.githubusercontent.com',
        'registry-1.docker.io',
        'auth.docker.io',
        'production.cloudflare.docker.com'
    );

-- Network rule allowing workers to reach Snowflake and external endpoints.
-- Workers execute DAG tasks that may connect to Snowflake (via snowflake.connector),
-- external APIs, dbt Cloud, SFTP servers, etc. Broad egress is intentional here;
-- restrict VALUE_LIST to specific hosts if your security policy requires it.
CREATE OR REPLACE NETWORK RULE AIRFLOW_SNOWFLAKE_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('0.0.0.0:443', '0.0.0.0:80');

-- External Access Integration using both network rules
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION AIRFLOW_EXTERNAL_ACCESS
    ALLOWED_NETWORK_RULES = (AIRFLOW_EGRESS_RULE, AIRFLOW_SNOWFLAKE_EGRESS_RULE)
    ENABLED = TRUE;
