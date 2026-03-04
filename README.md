# Apache Airflow 3.1.7 on Snowpark Container Services (SPCS)

Production-grade reference architecture for running Apache Airflow 3.1.7 on SPCS with CeleryExecutor, multi-pool compute isolation, and end-to-end DAG execution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWPARK CONTAINER SERVICES                   │
│                                                                 │
│  ┌─────────────┐  ┌──────────────────────────────────────────┐  │
│  │ INFRA_POOL   │  │ CORE_POOL (CPU_X64_M, 1-2 nodes)       │  │
│  │ CPU_X64_S    │  │                                          │  │
│  │ 1 node       │  │  AF_API_SERVER ──┐                      │  │
│  │              │  │  AF_SCHEDULER    │  Execution API (JWT)  │  │
│  │ AF_POSTGRES  │  │  AF_DAG_PROC    │                      │  │
│  │ AF_REDIS ────┼──│  AF_TRIGGERER   │                      │  │
│  │              │  │                  │                      │  │
│  └─────────────┘  └──────────────────┼───────────────────────┘  │
│                                      │                          │
│  ┌───────────────────────────────────┼───────────────────────┐  │
│  │ WORKER_POOL (CPU_X64_S, 1-5 nodes, auto-scaling)         │  │
│  │                                   │                       │  │
│  │  AF_WORKERS (CeleryExecutor) ─────┘                       │  │
│  │  Tasks execute here                                       │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Services (7 total)

| Service | Compute Pool | Purpose |
|---------|-------------|---------|
| AF_POSTGRES | INFRA_POOL | Airflow metadata database (block storage) |
| AF_REDIS | INFRA_POOL | Celery message broker |
| AF_API_SERVER | CORE_POOL | Web UI + Execution API (public endpoint) |
| AF_SCHEDULER | CORE_POOL | DAG scheduling and task orchestration |
| AF_DAG_PROCESSOR | CORE_POOL | DAG file parsing |
| AF_TRIGGERER | CORE_POOL | Async trigger handling |
| AF_WORKERS | WORKER_POOL | Task execution (auto-scales 1-5 instances) |

## Prerequisites

- Snowflake account with ACCOUNTADMIN role
- Docker Desktop (for building linux/amd64 images)
- `snow` CLI or Cortex Code for Snowflake operations

## Quick Start

> **Order matters.** Steps 1-2 prepare secrets and infrastructure. Step 3 builds Docker images (requires the image repo from step 2). Step 4 creates services (requires images from step 3). `deploy.sh` will abort with an actionable error if images are missing.

### 1. Generate secrets

```bash
# Auto-generate Fernet key, Postgres/Redis passwords, JWT secret
bash scripts/generate_secrets.sh
# Writes sql/03_setup_secrets.sql (gitignored, never committed)
```

> Requires `python3` with the `cryptography` package (`pip3 install cryptography`).
> No Snowflake credentials needed in the secrets — DAGs authenticate via SPCS native OAuth tokens automatically.

### 2. Create Snowflake objects and image repository

```bash
# Runs SQL 01-06: database, stages, secrets, networking, compute pools, image repo
# Also uploads specs and DAGs to stages
# Stops BEFORE creating services (that requires images)
bash scripts/deploy.sh --connection <connection>
```

On first run, `deploy.sh` will detect that images haven't been pushed yet and exit with instructions for step 3. This is expected — it means all Snowflake objects are ready for the image build.

### 3. Build and push Docker images

```bash
# Auto-detects registry URL from the connection
bash scripts/build_and_push.sh --connection <connection>
# Builds and pushes: airflow:3.1.7, airflow-postgres:17.9, airflow-redis:7.4
```

Or with an explicit registry URL:

```bash
bash scripts/build_and_push.sh <account>.registry.snowflakecomputing.com/airflow_db/airflow_schema/airflow_repository
```

### 4. Deploy services

```bash
# Re-run deploy.sh — this time images exist, so it creates all 7 services
bash scripts/deploy.sh --connection <connection>
```

`deploy.sh` is idempotent: re-running it after images are pushed will skip through the already-created objects and proceed to service creation.

### 5. Access the Airflow UI

```sql
SHOW ENDPOINTS IN SERVICE AIRFLOW_DB.AIRFLOW_SCHEMA.AF_API_SERVER;
```

The endpoint URL requires **two authentication steps**:
1. **Snowflake SSO** — authenticate via your Snowflake account
2. **Airflow Simple Auth Manager** — username: `admin`, password: `admin`

> **Security**: Change the default `admin/admin` password for any non-demo deployment. The password is set via `simple_auth_manager_passwords.json.generated` written by the entrypoint before the api-server starts. See `images/airflow/entrypoint.sh`.

## Project Structure

```
airflow-spcs-v3/
├── dags/                       # Airflow DAG definitions
│   ├── example_taskflow.py     # TaskFlow API example (extract→transform→load)
│   ├── example_snowflake.py    # Snowflake connectivity demo (SPCS OAuth)
│   └── snowflake_etl_pipeline.py # End-to-end ETL: ingest→transform→validate
├── images/                     # Docker images
│   ├── airflow/
│   │   ├── Dockerfile          # Based on apache/airflow:3.1.7
│   │   ├── entrypoint.sh       # Role-based entrypoint (db migrate + role dispatch)
│   │   └── requirements.txt    # Additional Python packages
│   ├── postgres/
│   │   └── Dockerfile          # PostgreSQL 17.9 with custom config
│   └── redis/
│       └── Dockerfile          # Redis 7.4 with password auth
├── specs/                      # SPCS service specifications (YAML)
│   ├── af_postgres.yaml
│   ├── af_redis.yaml
│   ├── af_api_server.yaml
│   ├── af_scheduler.yaml
│   ├── af_dag_processor.yaml
│   ├── af_triggerer.yaml
│   └── af_workers.yaml
├── sql/                        # Snowflake setup scripts (run in order)
│   ├── 01_setup_database.sql
│   ├── 02_setup_stages.sql
│   ├── 03_setup_secrets.sql.template  # Template — run generate_secrets.sh to produce .sql
│   ├── 04_setup_networking.sql
│   ├── 05_setup_compute_pools.sql
│   ├── 06_setup_image_repo.sql
│   ├── 07_create_services.sql
│   ├── 08_validate.sql
│   ├── 09_suspend_all.sql
│   └── 10_resume_all.sql
├── tests/                      # TDD test suite (pytest)
│   ├── test_spec_schemas.py    # SPCS spec structure validation
│   ├── test_service_connectivity.py  # Inter-service dependency checks
│   ├── test_env_config.py      # Environment variable validation
│   ├── test_docker_builds.py   # Dockerfile correctness
│   ├── test_dag_syntax.py      # DAG file validation
│   └── test_sql_objects.py     # SQL script validation
├── scripts/                    # Build/deploy automation
│   ├── build_and_push.sh       # Build and push Docker images to SPCS
│   ├── deploy.sh               # Full deployment pipeline
│   ├── generate_secrets.sh     # Auto-generate secrets SQL (Fernet, passwords, JWT)
│   └── teardown.sh             # Tear down all services and resources
├── .env.example                # Template for local config overrides
├── .gitignore
├── LICENSE                     # Apache 2.0
├── SPECIFICATION.md
└── CONSTITUTION.md
```

## Secrets

| Secret Name | Purpose |
|-------------|---------|
| `AIRFLOW_FERNET_KEY` | Encryption key for Airflow connections/variables |
| `AIRFLOW_POSTGRES_PWD` | PostgreSQL password |
| `AIRFLOW_REDIS_PWD` | Redis password |
| `AIRFLOW_JWT_SECRET` | Shared JWT secret for Execution API auth (64+ bytes required for SHA512) |

## Key Airflow 3.x Configuration

All Airflow services share these critical environment variables:

| Env Var | Value | Notes |
|---------|-------|-------|
| `AIRFLOW__CORE__EXECUTOR` | `CeleryExecutor` | Distributed execution via Redis |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://af-api-server:8080/execution/` | `/execution/` suffix required |
| `AIRFLOW__API_AUTH__JWT_SECRET` | `{{secret.airflow_jwt_secret.secret_string}}` | Must be identical across all services |
| `AIRFLOW__API__SECRET_KEY` | `{{secret.airflow_jwt_secret.secret_string}}` | Webserver session signing |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `False` | Auto-enable new DAGs |

## SPCS Gotchas

1. **DNS naming**: Service names with underscores get hyphenated DNS names (`AF_POSTGRES` → `af-postgres`)
2. **Readiness probes are HTTP-only**: Non-HTTP services (Postgres, Redis) must omit `readinessProbe`
3. **`protocol` field not supported**: Omit from both `readinessProbe` and `endpoints`
4. **Stage volume permissions**: Add `uid`/`gid` matching container user (Airflow: 50000/0)
5. **Block storage**: Requires `DROP SERVICE ... FORCE`; takes 60-300s to provision
6. **Execution API URL**: Must use `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` with `/execution/` suffix (NOT the Airflow 2.x `INTERNAL_API_URL`)
7. **JWT secret sharing**: `AIRFLOW__API_AUTH__JWT_SECRET` must be identical on all services or tasks fail with `Signature verification failed`
8. **Simple Auth Manager**: `airflow users create` is ignored; write `{"admin":"admin"}` to `/opt/airflow/simple_auth_manager_passwords.json.generated` before api-server start for fixed credentials

## Snowflake Connection (SPCS OAuth)

DAGs connect to Snowflake using the **SPCS native OAuth token** — no connection URI, passwords, or secrets needed.

Inside every SPCS container, Snowflake automatically provides:
- `/snowflake/session/token` — OAuth token file (auto-refreshed every few minutes)
- `SNOWFLAKE_ACCOUNT` env var — account identifier
- `SNOWFLAKE_HOST` env var — internal host for private connectivity

DAGs use `snowflake.connector.connect()` with `authenticator="oauth"` and the token from the file. See `dags/snowflake_etl_pipeline.py` for the pattern.

> **Note**: Snowflake DAGs only work when running inside SPCS. For local development, mock or stub the connection.

## Operations

```sql
-- Suspend all services (cost savings)
-- Run sql/09_suspend_all.sql

-- Resume all services
-- Run sql/10_resume_all.sql

-- Check service health
SELECT SYSTEM$GET_SERVICE_STATUS('AIRFLOW_DB.AIRFLOW_SCHEMA.AF_API_SERVER');

-- View logs
CALL SYSTEM$GET_SERVICE_LOGS('AIRFLOW_DB.AIRFLOW_SCHEMA.AF_WORKERS', 0, 'worker', 100);

-- Get Airflow UI URL
SHOW ENDPOINTS IN SERVICE AIRFLOW_DB.AIRFLOW_SCHEMA.AF_API_SERVER;
```

## Tests

```bash
pytest tests/ -v
# 143 passed, 1 skipped
```

Tests validate: spec schemas, inter-service connectivity, env var consistency, Dockerfile correctness, DAG syntax, and SQL script patterns.
