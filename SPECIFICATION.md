# Feature Specification: Airflow 3.1.7 Production Template on SPCS

**Created**: 2026-03-03
**Status**: Draft
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

## Success Criteria

### Measurable Outcomes

- **SC-001**: All 7 SPCS services reach READY status within 10 minutes of deployment
- **SC-002**: Sample DAG completes end-to-end (schedule → execute → log) within 5 minutes
- **SC-003**: Worker auto-scaling triggers within 3 minutes under concurrent task load
- **SC-004**: Full stack suspend completes within 2 minutes; resume within 5 minutes
- **SC-005**: All tests pass (spec validation, DAG parsing, Docker builds, SQL validation)
- **SC-006**: Template is forkable: a new user with ACCOUNTADMIN can deploy by following README alone
- **SC-007**: Idle credit cost is documented and matches estimates (< 1 credit/hour at minimum nodes)
