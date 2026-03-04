-- 05_setup_compute_pools.sql
-- Creates 3 compute pools for the Airflow SPCS deployment.
-- INFRA_POOL: postgres + redis (fixed, 1 node)
-- CORE_POOL: api-server, scheduler, dag-processor, triggerer (1-2 nodes)
-- WORKER_POOL: Celery workers (auto-scale 1-5 nodes)
-- Idempotent: uses IF NOT EXISTS.

USE ROLE ACCOUNTADMIN;

-- Infrastructure pool (postgres + redis)
-- CPU_X64_S: 3 vCPU, 13GB RAM - sufficient for PG17 + Redis
CREATE COMPUTE POOL IF NOT EXISTS INFRA_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 3600;

-- Core Airflow services pool
-- CPU_X64_M: 6 vCPU, 28GB RAM
-- 4 services (api-server, scheduler, dag-processor, triggerer) each need their own node
CREATE COMPUTE POOL IF NOT EXISTS CORE_POOL
    MIN_NODES = 1
    MAX_NODES = 4
    INSTANCE_FAMILY = CPU_X64_M
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 3600;

-- Worker pool (auto-scaling)
-- CPU_X64_S: 3 vCPU, 13GB RAM per worker node
CREATE COMPUTE POOL IF NOT EXISTS WORKER_POOL
    MIN_NODES = 1
    MAX_NODES = 5
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 1800;
