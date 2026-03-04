# Airflow SPCS v3 Constitution

## Core Principles

### I. Production-Grade Reference Architecture
This is a template others will fork. Every decision must be justified, documented, and represent a defensible production choice. No shortcuts that wouldn't survive a production review.

### II. Test-First (NON-NEGOTIABLE)
TDD mandatory: Tests written and failing before implementation. Red-Green-Refactor cycle strictly enforced.
- YAML service specs validated against schema before deployment
- DAGs parsed and validated against Airflow 3.x syntax before upload
- Dockerfiles linted and build-tested for `linux/amd64` before push
- SQL scripts validated for object existence and idempotency
- Cross-service connectivity tested after deployment

### III. Component Isolation
Each Airflow component runs as an independent SPCS service. Components communicate only via well-defined network interfaces (PostgreSQL wire protocol, Redis protocol, Airflow REST API). No shared-process coupling.

### IV. Version Pinning with Justification
Every dependency version is explicitly pinned and documented with rationale:
- **Airflow 3.1.7**: Latest stable major release
- **PostgreSQL 17.9**: Latest in Airflow's supported range (13-17); PG 18 excluded (documented)
- **Redis 7.4**: Latest stable; Celery 5.5+ compatible
- **Python 3.12**: Airflow 3.x recommended runtime

### V. Cost Awareness
SPCS compute pools cost credits when nodes exist (IDLE or ACTIVE). The template must:
- Use `AUTO_SUSPEND_SECS` on all compute pools
- Provide suspend/resume scripts for the full stack
- Size compute pools to minimum viable for each workload
- Document credit cost estimates for each pool configuration

### VI. Security by Default
- No secrets in Docker images or YAML specs; use Snowflake SECRET objects
- No internet egress unless explicitly configured via External Access Integration
- All images built for `linux/amd64` only (SPCS requirement)
- Fernet key for Airflow encryption configured as Snowflake secret

### VII. Idempotent Operations
All SQL scripts and deploy scripts must be safely re-runnable. Use `CREATE OR REPLACE`, `IF NOT EXISTS`, and `IF EXISTS` patterns throughout.

## Technology Constraints

- **Target Platform**: Snowpark Container Services (SPCS) on AWS
- **Snowflake Connection**: Your SPCS-enabled Snowflake connection
- **Role**: ACCOUNTADMIN
- **Executor**: CeleryExecutor (Redis broker + PostgreSQL backend)
- **Images**: Must be `--platform linux/amd64`
- **Storage**: Snowflake stages with `SNOWFLAKE_SSE` encryption for DAGs, logs, specs
- **DAG Sync**: Snowflake stage (`@AIRFLOW_DAGS`) + `sync_dags.sh` upload script

## Development Workflow

1. Write/update tests (RED)
2. Run tests, confirm failure (RED confirmed)
3. Implement minimum code to pass (GREEN)
4. Refactor if needed (REFACTOR)
5. Verify all tests pass
6. Document decisions in code comments and docs/

## Governance

- Constitution supersedes all other practices
- Architecture changes require updating this document first
- Version pins require documented justification for any change
- PG 18 incompatibility rationale must be maintained as versions evolve

**Version**: 1.0.0 | **Ratified**: 2026-03-03
