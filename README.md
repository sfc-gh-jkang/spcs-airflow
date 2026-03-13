# Apache Airflow 3.1.7 on Snowpark Container Services (SPCS)

Production-grade reference architecture for running Apache Airflow 3.1.7 on SPCS with CeleryExecutor, multi-pool compute isolation, and end-to-end DAG execution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWPARK CONTAINER SERVICES                   │
│                                                                 │
│  ┌─────────────┐  ┌──────────────────────────────────────────┐  │
│  │ INFRA_POOL   │  │ CORE_POOL (CPU_X64_M, 1-4 nodes)       │  │
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

> Requires `python3` with the `cryptography` package (`pip install cryptography` or `uv pip install cryptography`).
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

### Updating Existing Services

After the initial deployment, use `--update` to update services via `ALTER SERVICE`. This performs a rolling upgrade and **preserves the ingress URL** (no URL change).

```bash
# Update specs, DAGs, and ALTER SERVICE all 7 services (URL stays the same)
bash scripts/deploy.sh --connection <connection> --update
```

The `--update` flag:
- Skips infrastructure setup (SQL 01-06 — already exists)
- Uploads updated spec files and DAGs to stage
- Runs `ALTER SERVICE` on all 7 services (`sql/07b_update_services.sql`)
- Validates and prints the same stable endpoint URL

**Safety nets:**
- If you run `deploy.sh` without `--update` and services already exist, it warns you and suggests `--update`
- If you run `deploy.sh --update` but no services exist, it errors with instructions to do a first-time deploy

> **Do NOT use `CREATE OR REPLACE SERVICE`** — this drops and recreates the service, generating a new ingress URL and breaking any bookmarks or integrations.

### 5. Access the Airflow UI

```sql
SHOW ENDPOINTS IN SERVICE AIRFLOW_DB.AIRFLOW_SCHEMA.AF_API_SERVER;
```

The endpoint URL requires **two authentication steps**:
1. **Snowflake SSO** — authenticate via your Snowflake account
2. **Airflow Simple Auth Manager** — username: `admin`, password: `admin`

> **Security**: Change the default `admin/admin` password for any non-demo deployment. The password is set via `simple_auth_manager_passwords.json.generated` written by the entrypoint before the api-server starts. See `images/airflow/entrypoint.sh`.

## Dependencies

Additional Python packages are managed with [UV](https://github.com/astral-sh/uv) and `pyproject.toml` (not pip/requirements.txt). Versions are pinned to the [Airflow 3.1.7 constraints file](https://github.com/apache/airflow/blob/constraints-3.1.7/constraints-3.12.txt) for deterministic builds.

See `images/airflow/pyproject.toml` for the full list.

## Project Structure

```
airflow-spcs-v3/
├── dags/                       # Airflow DAG definitions
│   ├── utils/                  # Shared utilities for DAGs
│   │   ├── __init__.py
│   │   └── snowflake_conn.py   # Snowflake connection helper (SPCS/local auto-detect)
│   ├── example_taskflow.py     # TaskFlow API example (extract→transform→load)
│   ├── example_snowflake.py    # Snowflake connectivity demo (SPCS OAuth)
│   ├── snowflake_etl_pipeline.py # End-to-end ETL: ingest→transform→validate
│   └── e2e_snowflake_objects.py  # E2E test DAG: creates real Snowflake tables
├── images/                     # Docker images
│   ├── airflow/
│   │   ├── Dockerfile          # Based on apache/airflow:3.1.7
│   │   ├── entrypoint.sh       # Role-based entrypoint (db migrate + role dispatch)
│   │   └── pyproject.toml      # Python deps (UV, pinned to Airflow 3.1.7 constraints)
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
│   ├── 07_create_services.sql         # First-time only (CREATE SERVICE IF NOT EXISTS)
│   ├── 07b_update_services.sql        # Updates only (ALTER SERVICE — preserves URLs)
│   ├── 08_validate.sql
│   ├── 09_suspend_all.sql
│   └── 10_resume_all.sql
├── tests/                      # TDD test suite — 571+ tests (offline + 5 local + 6 E2E)
│   ├── conftest.py             # Shared fixtures and pytest markers
│   ├── test_spec_schemas.py    # SPCS spec structure validation
│   ├── test_service_connectivity.py  # Inter-service dependency checks
│   ├── test_env_config.py      # Environment variable validation
│   ├── test_docker_builds.py   # Dockerfile correctness
│   ├── test_dag_syntax.py      # DAG file validation
│   ├── test_sql_objects.py     # SQL script validation
│   ├── test_cross_file_consistency.py  # Spec↔SQL refs, pool assignments, versions
│   ├── test_entrypoint.py      # Role handling, db-migrate, auth JSON
│   ├── test_pyproject.py       # UV structure, deps, version pins
│   ├── test_shell_scripts.py   # Script structure, security, refs
│   ├── test_sync_dags_behavior.py  # Upload behavior, subdirs, pycache exclusion
│   ├── test_snowflake_conn.py  # Shared connection helper tests
│   ├── test_readme_accuracy.py # README refs files that exist, documents all features
│   ├── test_ci_config.py       # GitLab CI, .gitignore
│   ├── test_compose_config.py  # Docker-compose ↔ SPCS env var parity
│   ├── test_multi_container_consistency.py  # Cross-spec value consistency
│   ├── test_infrastructure_consistency.py   # Infra-layer consistency (secrets, images, ports)
│   ├── test_local_compose.py   # Local integration tests (docker-compose + REST API)
│   └── test_e2e_spcs.py        # End-to-end tests (live SPCS cluster)
├── scripts/                    # Build/deploy automation
│   ├── build_and_push.sh       # Build and push Docker images to SPCS
│   ├── deploy.sh               # Full deployment pipeline
│   ├── generate_secrets.sh     # Auto-generate secrets SQL (Fernet, passwords, JWT)
│   ├── sync_dags.sh            # Hot-reload DAGs to running cluster (no restart)
│   └── teardown.sh             # Tear down all services and resources
├── docker-compose.yaml         # Local dev stack (LocalExecutor, bind-mount DAGs)
├── pytest.ini                  # Default marker exclusion (bare pytest = offline only)
├── .env.example                # Template for local Snowflake credentials
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
9. **Stage mount v2 syntax required on GCP**: Use `source: stage` + `stageConfig: { name: "@stage" }` (v2), not `source: "@stage"` (v1). AWS/Azure accept both; GCP rejects v1.
10. **Compute pool sizing**: Each SPCS service needs its own node slot. A pool with `MAX_NODES=2` running 4 services will leave 2 services stuck PENDING. Set `MAX_NODES >= number of services` in that pool.
11. **`SYSTEM$REGISTRY_LIST_IMAGES` on GCP**: Returns `401 Unauthorized` on GCP accounts (platform bug). Use error handling or skip the image check on GCP.
12. **`DROP SERVICE` with block storage**: Services using `blockStorage` volumes require `DROP SERVICE ... FORCE` or `snapshotOnDelete=true` in the spec.
13. **Log symlinks crash on GCP stage mounts**: Airflow tries to symlink "latest" log directories. GCP stage mounts reject symlinks (`Errno 95`). Set `AIRFLOW__SCHEDULER__SYMLINK_LATEST_LOG=False` on all services, and don't mount the logs stage on the dag_processor (it only needs dags).
14. **Airflow 3.x connection validation on save**: Airflow 3.x eagerly tests connections when you click Save in the UI. If the API server lacks outbound egress (EAI), this produces a 500 error after ~15 seconds. This did NOT happen in Airflow 2.x (which saved without testing). Ensure `AF_API_SERVER` has the EAI attached.

## Networking & External Access

SPCS blocks all outbound traffic by default. The deployment creates two network rules and an External Access Integration (EAI) to enable outbound connectivity:

| Network Rule | Purpose | Hosts |
|-------------|---------|-------|
| `AIRFLOW_EGRESS_RULE` | Package registries for pip/Docker | pypi.org, github.com, Docker Hub |
| `AIRFLOW_SNOWFLAKE_EGRESS_RULE` | Worker/API server outbound access | `0.0.0.0:443`, `0.0.0.0:80` (all HTTPS/HTTP) |

The EAI (`AIRFLOW_EXTERNAL_ACCESS`) is attached to two services:

| Service | Why it needs EAI |
|---------|-----------------|
| `AF_API_SERVER` | Validates connections on save (Airflow 3.x tests connectivity eagerly) |
| `AF_WORKERS` | Executes DAG tasks that call `snowflake.connector`, external APIs, dbt Cloud, etc. |

Infrastructure services (postgres, redis, scheduler, dag-processor, triggerer) only use intra-SPCS DNS and do not need outbound access.

### Restricting egress (production hardening)

The default `AIRFLOW_SNOWFLAKE_EGRESS_RULE` allows all outbound HTTPS/HTTP. For production, replace `0.0.0.0` with specific hosts:

```sql
CREATE OR REPLACE NETWORK RULE AIRFLOW_SNOWFLAKE_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        '<orgname>-<accountname>.snowflakecomputing.com',
        'cloud.getdbt.com'  -- if using dbt Cloud
    );
```

### Adding custom egress rules

To allow workers to reach additional endpoints (SFTP servers, external APIs, etc.), either add hosts to `AIRFLOW_SNOWFLAKE_EGRESS_RULE` or create additional network rules and add them to the EAI:

```sql
ALTER EXTERNAL ACCESS INTEGRATION AIRFLOW_EXTERNAL_ACCESS
  SET ALLOWED_NETWORK_RULES = (
    AIRFLOW_EGRESS_RULE,
    AIRFLOW_SNOWFLAKE_EGRESS_RULE,
    MY_CUSTOM_RULE
  );
```

## Snowflake Connection (SPCS OAuth + Local)

DAGs connect to Snowflake using a **shared connection helper** (`dags/utils/snowflake_conn.py`) that auto-detects the environment:

- **On SPCS**: Uses the native OAuth token from `/snowflake/session/token` — no passwords or secrets needed
- **Locally**: Uses `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` env vars from `.env`

```python
from utils.snowflake_conn import get_snowflake_connection, run_sql

conn = get_snowflake_connection()                    # auto-detects SPCS vs local
conn = get_snowflake_connection(database="MY_DB")    # kwargs override defaults
results = run_sql("SELECT CURRENT_VERSION()", fetch=True)
```

> **Note**: The `example_taskflow` DAG works without any Snowflake connection. The `example_snowflake` and `snowflake_etl_pipeline` DAGs require either SPCS or local Snowflake credentials.

## Local Development & DAG Deployment

DAGs are deployed via a Snowflake internal stage (`AIRFLOW_DAGS`) that is mounted into all Airflow containers at `/opt/airflow/dags`. **No service restart or image rebuild is needed** — upload the file and the dag-processor picks it up automatically.

### Quick sync (recommended)

```bash
# Sync all DAGs from dags/ to the running cluster
bash scripts/sync_dags.sh --connection <connection>

# Sync a single file
bash scripts/sync_dags.sh --connection <connection> my_new_dag.py

# Sync specific files
bash scripts/sync_dags.sh --connection <connection> dag_a.py dag_b.py
```

### Manual sync via Snow CLI

```bash
snow sql --connection <connection> -q \
  "PUT file://$(pwd)/dags/my_dag.py @AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_DAGS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
```

### Development workflow

```
 ┌──────────────┐    PUT to stage     ┌────────────────┐    auto-sync     ┌─────────────┐
 │ Local editor  │ ──────────────────> │  @AIRFLOW_DAGS │ ──────────────> │ All SPCS     │
 │ dags/*.py     │   (sync_dags.sh)   │  (internal     │  (~30-60 sec)   │ containers   │
 └──────────────┘                     │   stage)       │                 │ /opt/airflow/ │
                                      └────────────────┘                 │  dags/       │
                                                                         └─────────────┘
```

1. **Edit locally** — write or modify DAG files in the `dags/` directory
2. **Sync** — run `bash scripts/sync_dags.sh --connection <name>` (takes ~2-5 seconds)
3. **Wait** — SPCS stage volumes auto-sync to containers within ~30-60 seconds
4. **Verify** — check the Airflow UI or dag-processor logs:
   ```bash
   snow sql --connection <connection> -q \
     "CALL SYSTEM\$GET_SERVICE_LOGS('AIRFLOW_DB.AIRFLOW_SCHEMA.AF_DAG_PROCESSOR', 0, 'dag-processor', 50);"
   ```

### What requires a full redeploy

| Change | Action Required |
|--------|----------------|
| New/modified DAG file | `sync_dags.sh` only (no restart) |
| New Python dependency | Rebuild Docker image + `build_and_push.sh` + `deploy.sh --update` |
| Airflow config change (env var) | Update spec YAML + `deploy.sh --update` |
| Spec YAML change (resources, volumes) | `deploy.sh --update` (ALTER SERVICE, preserves URL) |
| New Snowflake secret | Run secret SQL + `deploy.sh --update` |

### Removing a DAG

```sql
-- Remove a DAG file from the stage
REMOVE @AIRFLOW_DB.AIRFLOW_SCHEMA.AIRFLOW_DAGS/old_dag.py;
```

The dag-processor will detect the removal and mark the DAG as "missing" in the UI. You may also want to delete the DAG metadata from the Airflow UI (DAGs > kebab menu > Delete).

### Local testing (without SPCS)

DAGs that use SPCS-native OAuth (`/snowflake/session/token`) only work inside SPCS containers. For local syntax/import validation:

```bash
# Validate DAG syntax without running
python -c "import ast; ast.parse(open('dags/my_dag.py').read()); print('OK')"

# Run the full test suite (includes DAG syntax checks)
pytest tests/test_dag_syntax.py -v
```

### Local Development with Docker Compose

For full local DAG development and execution without SPCS, use the included `docker-compose.yaml`. It runs the same Airflow Docker image with LocalExecutor (no Celery/Redis needed).

```bash
# 1. Set up credentials
cp .env.example .env
# Edit .env — fill in SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD

# 2. Start the local stack
docker compose up -d

# 3. Open the Airflow UI
open http://localhost:8080    # admin / admin
```

DAGs are bind-mounted from `./dags` — edit locally and changes appear immediately (no rebuild, no sync).

**What's included:**

| Service | Purpose |
|---------|---------|
| `postgres` | Airflow metadata database |
| `airflow-init` | One-shot: runs `db migrate` + creates admin user |
| `airflow-webserver` | Airflow API server + UI on port 8080 |
| `airflow-scheduler` | DAG scheduling + task execution (LocalExecutor) |
| `airflow-dag-processor` | DAG file parsing (separate from scheduler in Airflow 3.x) |

**Rebuilding after dependency changes:**

```bash
docker compose build --no-cache
docker compose up -d
```

**Stopping:**

```bash
docker compose down        # stop and remove containers
docker compose down -v     # also remove postgres volume (full reset)
```

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

Three test tiers, from fastest to most comprehensive:

### Offline Tests (~8s)

Static validation of every artifact — no Docker, no network, no SPCS required:

```bash
pytest tests/ -v
# 571 passed, 1 skipped
```

A bare `pytest` (no flags) runs only offline tests — `pytest.ini` excludes `e2e` and `local` markers by default.

### Local Integration Tests (5 tests, ~65s)

Spins up the full Airflow stack via `docker-compose.yaml`, exercises the DAG lifecycle via the Airflow 3.x REST API, then tears everything down:

```bash
pytest tests/test_local_compose.py -m local -v
```

**Requirements**: Docker Desktop running, ports 8080 + 5432 free.

**What it tests**:
- Health endpoint responds (`/api/v2/monitor/health`)
- DAGs are parsed and listed via REST API
- `example_taskflow` DAG triggers and completes with `state=success`
- All 3 task instances (extract → transform → load) succeed
- XCom data flows between tasks

### End-to-End Tests (6 tests, ~4min, requires live SPCS)

The E2E test suite (`tests/test_e2e_spcs.py`) verifies the full deployment pipeline against a running SPCS cluster:

1. Uploads DAGs via `sync_dags.sh` (including `utils/` subdirectory)
2. Verifies stage contents via `LIST @AIRFLOW_DAGS`
3. Confirms the dag-processor has parsed DAGs (via `EXECUTE JOB SERVICE`)
4. Triggers `example_taskflow` and polls until the run succeeds
5. Triggers `e2e_snowflake_objects` — a DAG that creates a real Snowflake table, inserts data, and self-validates — then queries the table directly via `snow sql` to confirm the objects exist with expected data

```bash
# Requires a running SPCS cluster and a snow CLI connection named 'aws_spcs'
pytest tests/test_e2e_spcs.py -m e2e -v
```

E2E tests are excluded from normal `pytest` runs. They use `EXECUTE JOB SERVICE` on `WORKER_POOL` to run Airflow CLI commands inside one-shot containers connected to the shared metadata database.
