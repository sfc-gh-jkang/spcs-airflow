# PostgreSQL 18 Incompatibility with Apache Airflow 3.1.7

## Summary

Apache Airflow 3.1.7 does **not** support PostgreSQL 18. The officially supported
versions are **PostgreSQL 13, 14, 15, 16, and 17**. This template uses
**PostgreSQL 17.9**.

## Why PG 18 Is Excluded

### 1. Not in Airflow's CI Test Matrix

Airflow's CI pipeline tests against PG 13-17 only. PG 18 has not been validated
and is not listed in the `setup.cfg` or `pyproject.toml` supported databases.

### 2. Breaking Changes in PG 18

PostgreSQL 18 (released September 2025, current 18.3) introduced several changes
that affect Airflow's metadata database operations:

| Change | Impact on Airflow |
|--------|-------------------|
| **Data checksums enabled by default** | Performance overhead on Airflow's high-frequency metadata writes (task instance updates, XCom, DagRun state transitions). PG 13-17 default to checksums off. |
| **VACUUM inheritance behavior** | `VACUUM` now processes partitioned table hierarchies differently. Airflow's `task_instance` and `dag_run` tables could be affected if partitioning is used. |
| **MD5 password authentication deprecated** | PG 18 deprecates `md5` in favor of `scram-sha-256`. Airflow's default connection strings may fail if the PG server rejects MD5 auth. |
| **New Asynchronous I/O (AIO) subsystem** | The new `io_method` parameter changes how PG handles disk I/O. Untested interaction with Airflow's connection pooling (SQLAlchemy + pgbouncer patterns). |
| **XCom JSONB migration risk** | Airflow 3.x migrated XCom storage to JSONB. PG 18's JSONB handling changes (expanded indexing, new operators) are untested with Airflow's ORM layer. |

### 3. psycopg2 / psycopg Compatibility

The `psycopg2-binary` driver used by this template (and Airflow's default) has
not been fully certified against PG 18's wire protocol changes. While likely
compatible, edge cases in prepared statement handling and extended query protocol
have not been validated.

## Recommendation

Use **PostgreSQL 17.x** (this template uses 17.9) until:

1. Airflow's CI matrix officially adds PG 18
2. The Airflow community publishes PG 18 compatibility notes
3. psycopg2/psycopg3 drivers are certified for PG 18

## References

- [Airflow 3.x Supported Databases](https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html)
- [PostgreSQL 18 Release Notes](https://www.postgresql.org/docs/18/release-18.html)
- [Airflow Database Backend Configuration](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html)
