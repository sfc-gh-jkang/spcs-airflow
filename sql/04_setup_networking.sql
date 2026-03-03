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

-- External Access Integration using the network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION AIRFLOW_EXTERNAL_ACCESS
    ALLOWED_NETWORK_RULES = (AIRFLOW_EGRESS_RULE)
    ENABLED = TRUE;
