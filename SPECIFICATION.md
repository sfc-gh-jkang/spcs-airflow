# Feature Specification: Airflow 3.1.7 Production Template on SPCS

**Created**: 2026-03-03
**Status**: 1.0
**Input**: Production-grade reference architecture for Apache Airflow 3.1.7 on Snowpark Container Services

## User Scenarios & Testing

### User Story 1 - Deploy Full Airflow Stack (Priority: P1)

A platform engineer forks this template and deploys a complete Airflow 3.1.7 stack to their Snowflake SPCS account by running the SQL setup scripts and deploy script.

**Why this priority**: Without a working deployment, nothing else matters. This is the core value proposition.

**Independent Test**: Run `deploy.sh` against a clean Snowflake account with ACCOUNTADMIN; all 7 SPCS services reach READY status; Airflow UI accessible via public endpoint.

**Acceptance Scenarios**:

1. **Given** a Snowflake account with SPCS enabled and ACCOUNTADMIN role, **When** the user runs all SQL scripts in order and executes `deploy.sh`, **Then** all 7 SPCS services report READY via `SYSTEM$GET_SERVICE_STATUS`.
2. **Given** all services are running, **When** the user accesses the api-server public endpoint, **Then** the Airflow 3.x UI loads and displays no DAG parsing errors.
3. **Given** a fresh deployment, **When** `airflow db migrate` runs in the api-server init container, **Then** the PostgreSQL 17 metadata database is fully initialized without migration errors.

---

### User Story 2 - Run a Sample DAG End-to-End (Priority: P1)

A platform engineer triggers the included sample DAG and sees it execute successfully through the CeleryExecutor pipeline (scheduler → Redis → worker → result in metadata DB).

**Why this priority**: Proves the entire pipeline works, not just that services start.

**Independent Test**: Enable `example_taskflow` DAG in UI; trigger a run; verify task instances complete with SUCCESS status.

**Acceptance Scenarios**:

1. **Given** all services are running, **When** the user enables and triggers `example_taskflow` DAG, **Then** all task instances complete with SUCCESS status within 5 minutes.
2. **Given** a running DAG, **When** a task executes on a Celery worker, **Then** task logs are written to the `@airflow_logs` Snowflake stage and viewable in the UI.

---

### User Story 3 - Auto-Scale Workers Under Load (Priority: P2)

When multiple DAGs run concurrently and the single worker node is saturated, SPCS auto-scales the worker compute pool to additional nodes, and new Celery workers pick up queued tasks.

**Why this priority**: Core production differentiator vs. single-service design.

**Independent Test**: Trigger 10+ concurrent task instances; observe WORKER_POOL scale from 1 to 2+ nodes; tasks distributed across workers.

**Acceptance Scenarios**:

1. **Given** WORKER_POOL has MIN_NODES=1 and MAX_NODES=5, **When** 20 concurrent tasks are queued, **Then** SPCS adds at least 1 additional worker node and tasks are distributed.
2. **Given** extra worker nodes are running, **When** all tasks complete and workers idle, **Then** SPCS scales WORKER_POOL back to MIN_NODES after idle timeout.

---

### User Story 4 - Suspend and Resume Full Stack (Priority: P2)

A platform engineer suspends the entire Airflow stack to stop credit consumption, then resumes it later with all state preserved.

**Why this priority**: Cost management is critical for production SPCS deployments.

**Independent Test**: Run `09_suspend_all.sql`; verify all pools show SUSPENDED; run `10_resume_all.sql`; verify all services recover to READY; previously completed DAG runs still visible in UI.

**Acceptance Scenarios**:

1. **Given** all services are running, **When** the user runs `09_suspend_all.sql`, **Then** all 3 compute pools reach SUSPENDED state and no credits are consumed.
2. **Given** a suspended stack, **When** the user runs `10_resume_all.sql`, **Then** all 7 services return to READY within 5 minutes and the Airflow UI shows prior DAG run history.

---

### User Story 5 - Independent Service Restart (Priority: P3)

A platform engineer restarts a single service (e.g., scheduler) without affecting other services. The restarted service reconnects to postgres/redis and resumes operation.

**Why this priority**: Demonstrates the value of component isolation for production operations.

**Independent Test**: Drop and recreate `af_scheduler` service; verify scheduler reconnects and resumes scheduling without data loss.

**Acceptance Scenarios**:

1. **Given** all services are running, **When** the user drops and recreates `af_scheduler`, **Then** the scheduler reconnects to postgres and redis within 60 seconds and resumes scheduling.
2. **Given** a scheduler restart, **When** a DAG was mid-execution, **Then** the worker completes the in-progress task and the scheduler picks up remaining tasks after restart.

---

### Edge Cases

- What happens when PostgreSQL container restarts? (All Airflow components should reconnect via SQLAlchemy retry)
- What happens when Redis container restarts? (Celery workers reconnect; in-flight task state may be lost; tasks should be retried)
- What happens when a DAG has a syntax error? (dag-processor logs the error; other DAGs continue to be scheduled)
- What happens when WORKER_POOL reaches MAX_NODES? (Tasks queue in Redis until a worker slot becomes available)
- What happens when the api-server is unreachable? (Workers continue executing tasks; UI unavailable; tasks complete but logs may not be viewable until api-server returns)

## Requirements

### Functional Requirements

- **FR-001**: System MUST deploy 7 independent SPCS services: af_postgres, af_redis, af_api_server, af_scheduler, af_dag_processor, af_triggerer, af_workers
- **FR-002**: System MUST use 3 compute pools: INFRA_POOL (postgres+redis), CORE_POOL (api-server+scheduler+dag-processor+triggerer), WORKER_POOL (workers)
- **FR-003**: WORKER_POOL MUST auto-scale between MIN_NODES=1 and MAX_NODES=5
- **FR-004**: All compute pools MUST have AUTO_SUSPEND_SECS configured
- **FR-005**: System MUST store secrets (fernet key, postgres password, redis password) as Snowflake SECRET objects
- **FR-006**: System MUST use Snowflake stages for DAG files (`@airflow_dags`) and task logs (`@airflow_logs`)
- **FR-007**: System MUST store DAG files and logs on Snowflake stages (`@airflow_dags`, `@airflow_logs`)
- **FR-008**: All Docker images MUST be built for `linux/amd64` platform
- **FR-009**: System MUST use CeleryExecutor with Redis as broker and PostgreSQL as result backend
- **FR-010**: System MUST include suspend/resume SQL scripts for cost management
- **FR-011**: All SQL scripts MUST be idempotent (safe to re-run)
- **FR-012**: System MUST use Airflow 3.1.7, PostgreSQL 17.9, Redis 7.4
- **FR-013**: System MUST NOT support or include PostgreSQL 18 (documented incompatibility)
- **FR-014**: Sample DAGs MUST use Airflow 3.x TaskFlow SDK syntax (`from airflow.sdk import ...`)
- **FR-015**: System MUST include an External Access Integration for outbound network access (pip installs, git-sync)

### Key Entities

- **Compute Pool**: SPCS resource allocation unit; maps to VM nodes; 3 pools with different sizing
- **Service**: SPCS long-running container deployment; 7 services each with a YAML spec
- **Service Spec**: YAML file defining containers, endpoints, volumes, secrets for an SPCS service
- **Stage**: Snowflake storage for specs, DAGs, and logs
- **Secret**: Snowflake-managed credential store for passwords and keys
- **Image Repository**: Snowflake container registry for Docker images

## Test Coverage Matrix

### What We Test (542 tests total)

#### Offline Tests (531 tests, `-m "not e2e and not local"`, ~8s)

Static validation of every artifact without requiring a live SPCS cluster:

| Test File | Count | What It Covers | Spec Coverage |
|---|---|---|---|
| `test_spec_schemas.py` | ~65 | YAML spec structure, required keys, image refs, volumes, block storage, secrets, Redis command | FR-001, FR-002, FR-005, FR-006, FR-008 |
| `test_service_connectivity.py` | ~12 | Inter-service dependency refs (postgres/redis hostnames + ports), no localhost refs | FR-001, FR-009 |
| `test_env_config.py` | ~30 | Required env vars per service, Celery broker vars, readiness probes, public endpoints, stage volume uid/gid, secret consistency, executor config | FR-005, FR-006, FR-009 |
| `test_docker_builds.py` | ~18 | Dockerfile existence, FROM instructions, base images, version pins, security (no secrets), UV install, pyproject.toml copy | FR-008, FR-012 |
| `test_dag_syntax.py` | ~45 | DAG Python syntax, no deprecated imports, Airflow 3.x patterns, dag_id/schedule/start_date/catchup/tags, code quality | FR-014 |
| `test_sql_objects.py` | ~45 | SQL file existence, idempotency patterns, no hardcoded secrets, object names, service creation, suspend/resume coverage, compute pools, validation script, secrets template | FR-001, FR-002, FR-005, FR-010, FR-011 |
| `test_cross_file_consistency.py` | ~12 | Spec filenames match SQL refs, all specs referenced, service coverage in suspend/resume, compute pool assignments, version consistency, block storage config | FR-001, FR-002, FR-010 |
| `test_entrypoint.py` | ~12 | Bash shebang, set -e, role handling (all 6 roles + unknown), db-migrate logic, auth JSON | FR-001 |
| `test_pyproject.py` | ~8 | Project structure, dependencies, python version, provider packages, version pinning, no requirements.txt | FR-012 |
| `test_shell_scripts.py` | ~25 | Script existence, shebang, strict mode, no secrets, build refs, teardown refs, deploy refs, sync_dags behavior | FR-010 |
| `test_sync_dags_behavior.py` | ~15 | DAG upload behavior: top-level files, subdirectories, stage subpaths, file count, connection, auto_compress, overwrite, pycache exclusion, specific files, empty dir, output messages | FR-006 |
| `test_snowflake_conn.py` | ~5 | SPCS detection, OAuth on SPCS, env vars locally | FR-001 |
| `test_readme_accuracy.py` | ~22 | README exists, all 7 services mentioned, 4 secrets documented, 20+ referenced files exist, content checks (UV, pyproject, auth, executor, gotchas, DAG deployment, docker-compose, E2E tests, EXECUTE JOB SERVICE) | SC-006 |
| `test_ci_config.py` | ~6 | GitLab CI exists, valid YAML, test stage, pytest job, Python 3.12 image, .gitignore patterns | SC-005 |
| `test_compose_config.py` | ~35 | Docker-compose env var parity with SPCS specs, required services, Airflow 3.x requirements (Execution API URL, JWT secret, dag-processor), healthchecks, dependency ordering, LocalExecutor validation | FR-001, FR-014 |
| `test_multi_container_consistency.py` | ~35 | Cross-spec env var VALUE consistency (fernet, JWT, DB conn, dags folder, execution URL), secret template consistency, volume mount/stage name parity, log volume assignments, DAGS_FOLDER↔mountPath alignment, entrypoint role alignment | FR-001, FR-005, FR-006 |
| `test_infrastructure_consistency.py` | ~36 | Redis/Postgres password secret parity (server↔client), API_SECRET_KEY↔JWT_SECRET alignment, spec secrets↔SQL definitions, image path↔build script parity, build script↔Dockerfile version pins, PG18 exclusion, API server port consistency (entrypoint↔endpoint↔probe↔URL), entrypoint role safety, generate_secrets.sh↔template placeholder symmetry | FR-001, FR-005, FR-008, FR-012 |

#### E2E Tests (6 tests, `-m e2e`, ~4 min, requires live SPCS cluster)

Live validation against a running SPCS deployment using `EXECUTE JOB SERVICE` on `WORKER_POOL`:

| Test | What It Proves | Spec Coverage |
|---|---|---|
| `TestStageUpload::test_sync_dags_succeeds` | `sync_dags.sh` uploads DAGs to `@AIRFLOW_DAGS` without error | FR-006, US-1 |
| `TestStageUpload::test_stage_has_top_level_dags` | Top-level DAG files (example_taskflow.py, snowflake_etl_pipeline.py) appear on stage | FR-006, US-1 |
| `TestStageUpload::test_stage_has_utils_subdir` | `utils/__init__.py` and `utils/snowflake_conn.py` appear on stage | FR-006, US-1 |
| `TestDagParsing::test_dag_is_parsed` | `airflow dags list` inside a container finds `example_taskflow` — proving the dag-processor has parsed DAGs from the stage volume | US-1 (scenario 2), US-2 |
| `TestDagExecution::test_trigger_and_complete` | Triggers `example_taskflow`, polls until `state=success` — proving the full pipeline works (scheduler → metadata DB → task execution → success) | **US-2 (scenario 1)**, SC-002 |
| `TestSnowflakeObjects::test_trigger_creates_snowflake_objects` | Triggers `e2e_snowflake_objects`, polls until success, then queries Snowflake directly (`SELECT COUNT(*)`, `SELECT SUM(value)`) to verify the DAG created a real table with expected data. Proves the full loop: local DAG → stage upload → dag-processor parse → worker execution → SPCS OAuth → Snowflake DDL/DML | **US-2 (scenario 1)**, FR-006, FR-014 |

#### Local Integration Tests (5 tests, `-m local`, ~65s, requires Docker Desktop)

Validates the full Airflow 3.x DAG lifecycle locally using `docker-compose.yaml` and the REST API:

| Test | What It Proves | Spec Coverage |
|---|---|---|
| `TestLocalAirflowStack::test_health_endpoint` | Airflow api-server health endpoint (`/api/v2/monitor/health`) returns healthy status with metadatabase and scheduler components | FR-001 |
| `TestLocalAirflowStack::test_dags_are_loaded` | DAGs are parsed and listed via REST API — proves the dag-processor service works with bind-mounted DAGs | FR-014, US-2 |
| `TestLocalAirflowStack::test_dag_run_succeeds` | `example_taskflow` DAG triggers via REST API and completes with `state=success` — proves full execution pipeline: api-server → scheduler → LocalExecutor → Execution API | US-2 (scenario 1) |
| `TestLocalAirflowStack::test_all_task_instances_succeeded` | All 3 task instances (extract → transform → load) complete with `success` state | US-2 (scenario 1) |
| `TestLocalAirflowStack::test_xcom_data_flowed` | XCom entries exist for `extract` and `transform` tasks — proves TaskFlow data passing works end-to-end | US-2, FR-014 |

**Local Test Technical Details**:
- Uses `docker compose up -d --build --wait` with fallback health polling
- Authenticates via Airflow 3.x JWT: `POST /auth/token` → Bearer token for all API calls
- Triggers DAGs via `POST /api/v2/dags/<id>/dagRuns` with required `logical_date` field
- Polls `GET /api/v2/dags/<id>/dagRuns/<run_id>` until state is `success` or `failed`
- Fixture teardown runs `docker compose down -v` to clean up
- Marker: `@pytest.mark.local` — excluded from normal `pytest` runs

**E2E Technical Details**:
- Uses `EXECUTE JOB SERVICE` on `WORKER_POOL` (CORE_POOL is at capacity with 4 services)
- Secret template syntax: `{{secret.airflow_postgres_pwd.secret_string}}` in env vars
- Single-container trigger+poll design avoids ~90s cold-start penalty per poll iteration
- Logs fetched via `SYSTEM$GET_SERVICE_LOGS` in JSON format for reliable parsing
- Marker: `@pytest.mark.e2e` — excluded from normal `pytest` runs

### What We Do NOT Test (Gaps)

These items from the specification are **not yet covered by automated tests**:

| Gap | Spec Reference | Why It's Missing | Suggested Approach |
|---|---|---|---|
| All 7 services reach READY status | US-1 scenario 1, SC-001 | Requires running `SYSTEM$GET_SERVICE_STATUS` for each service; cluster already deployed so this is implicitly true when E2E tests pass | E2E test: query `SYSTEM$GET_SERVICE_STATUS` for all 7 services |
| Airflow UI loads without errors | US-1 scenario 2 | UI is behind Snowflake SSO; no headless browser access from CLI | Manual verification or Playwright via `cortex browser` |
| Task logs written to `@airflow_logs` stage | US-2 scenario 2 | Would need to `LIST @AIRFLOW_LOGS` after a DAG run and find matching log files | E2E test: after trigger succeeds, check `LIST @AIRFLOW_LOGS` for task log files |
| Worker auto-scaling under load | US-3, SC-003 | Requires triggering 20+ concurrent tasks, waiting for WORKER_POOL node count to increase, then verifying scale-down. Expensive and slow (~10+ min) | E2E test: trigger N parallel tasks, poll `SHOW COMPUTE POOLS` for node count > MIN_NODES |
| Suspend/resume preserves state | US-4, SC-004 | Destructive test — would suspend the live cluster, breaking other tests. Requires isolated test environment | Separate E2E suite with dedicated cluster lifecycle |
| Independent service restart | US-5 | Destructive — dropping a service mid-test affects other tests running against the same cluster | Separate E2E suite or run in isolation |
| PostgreSQL/Redis reconnection after restart | Edge case | Same as above — destructive to running services | Chaos-testing suite |
| DAG syntax error handling | Edge case | Would need to upload a broken DAG, verify dag-processor logs the error, then verify other DAGs still schedule | E2E test: upload broken DAG, check dag-processor logs, trigger a good DAG |
| Credit cost estimation | SC-007 | Requires `SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY` access and a known-idle period | Manual verification or scheduled monitoring query |

## Success Criteria

### Measurable Outcomes

- **SC-001**: All 7 SPCS services reach READY status within 10 minutes of deployment
- **SC-002**: Sample DAG completes end-to-end (schedule → execute → log) within 5 minutes — **TESTED (E2E)**
- **SC-003**: Worker auto-scaling triggers within 3 minutes under concurrent task load — **NOT TESTED**
- **SC-004**: Full stack suspend completes within 2 minutes; resume within 5 minutes — **NOT TESTED**
- **SC-005**: All tests pass (spec validation, DAG parsing, Docker builds, SQL validation) — **TESTED (531 offline + 5 local + 6 E2E = 542 total)**
- **SC-006**: Template is forkable: a new user with ACCOUNTADMIN can deploy by following README alone — **PARTIALLY TESTED (README accuracy tests)**
- **SC-007**: Idle credit cost is documented and matches estimates (< 1 credit/hour at minimum nodes) — **NOT TESTED**
