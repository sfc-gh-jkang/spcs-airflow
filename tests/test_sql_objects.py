"""Tests for SQL setup scripts.

Validates that:
- All 10 SQL scripts exist in correct order
- Scripts use idempotent patterns (CREATE OR REPLACE, IF NOT EXISTS, IF EXISTS)
- Scripts don't contain hardcoded secrets
- Scripts reference correct Snowflake object names
"""

import os
import re
import pytest

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")

EXPECTED_SQL_FILES = [
    "01_setup_database.sql",
    "02_setup_stages.sql",
    "03_setup_secrets.sql.template",
    "04_setup_networking.sql",
    "05_setup_compute_pools.sql",
    "06_setup_image_repo.sql",
    "07_create_services.sql",
    "07b_update_services.sql",
    "08_validate.sql",
    "09_suspend_all.sql",
    "10_resume_all.sql",
]


class TestSqlFilesExist:
    """All SQL setup scripts must exist."""

    @pytest.mark.parametrize("filename", EXPECTED_SQL_FILES)
    def test_sql_file_exists(self, filename):
        path = os.path.join(SQL_DIR, filename)
        assert os.path.isfile(path), f"Missing SQL script: {filename}"


class TestSqlIdempotency:
    """SQL scripts must use idempotent patterns."""

    IDEMPOTENT_PATTERNS = [
        r"CREATE\s+OR\s+REPLACE",
        r"IF\s+NOT\s+EXISTS",
        r"IF\s+EXISTS",
    ]

    @pytest.mark.parametrize("filename", EXPECTED_SQL_FILES)
    def test_uses_idempotent_patterns(self, filename):
        """DDL scripts (01-07) must use idempotent SQL patterns."""
        if filename in ("08_validate.sql",):
            pytest.skip("Validation script doesn't need idempotent DDL")
        path = os.path.join(SQL_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read().upper()

        # Skip if file is only SELECT/SHOW/DESCRIBE statements (validation/status)
        ddl_keywords = ["CREATE", "ALTER", "DROP", "GRANT"]
        has_ddl = any(kw in content for kw in ddl_keywords)
        if not has_ddl:
            return  # No DDL to check

        has_idempotent = any(
            re.search(pattern, content) for pattern in self.IDEMPOTENT_PATTERNS
        )
        assert has_idempotent, (
            f"{filename}: DDL scripts must use idempotent patterns "
            "(CREATE OR REPLACE, IF NOT EXISTS, IF EXISTS)"
        )


class TestSqlSecurity:
    """SQL scripts must not contain hardcoded secrets."""

    FORBIDDEN_PATTERNS = [
        # Literal password values (not variable references)
        r"PASSWORD\s*=\s*'[^'{$][^']*'",
        r"SECRET_STRING\s*=\s*'[^']*'",
    ]

    @pytest.mark.parametrize("filename", EXPECTED_SQL_FILES)
    def test_no_hardcoded_secrets(self, filename):
        path = os.path.join(SQL_DIR, filename)
        if not os.path.isfile(path):
            pytest.skip(f"{filename} not yet created")
        with open(path) as f:
            content = f.read()

        # Filter out comment lines
        non_comment_lines = [
            line
            for line in content.splitlines()
            if not line.strip().startswith("--")
        ]
        non_comment_content = "\n".join(non_comment_lines)

        for pattern in self.FORBIDDEN_PATTERNS:
            matches = re.findall(pattern, non_comment_content, re.IGNORECASE)
            # Allow placeholder patterns like 'CHANGE_ME' or '<your_password>'
            real_matches = [
                m for m in matches
                if "CHANGE_ME" not in m.upper()
                and "<" not in m
                and "REPLACE" not in m.upper()
            ]
            assert len(real_matches) == 0, (
                f"{filename}: possible hardcoded secret found: {real_matches}"
            )


class TestSqlObjectNames:
    """SQL scripts must reference the correct Snowflake object names."""

    EXPECTED_OBJECTS = {
        "AIRFLOW_DB": ["01_setup_database.sql"],
        "AIRFLOW_SCHEMA": ["01_setup_database.sql"],
        "AIRFLOW_REPOSITORY": ["06_setup_image_repo.sql"],
        "AIRFLOW_EGRESS_RULE": ["04_setup_networking.sql"],
        "AIRFLOW_SNOWFLAKE_EGRESS_RULE": ["04_setup_networking.sql"],
        "AIRFLOW_EXTERNAL_ACCESS": ["04_setup_networking.sql"],
    }

    @pytest.mark.parametrize(
        "object_name,expected_files", list(EXPECTED_OBJECTS.items())
    )
    def test_object_referenced(self, object_name, expected_files):
        for filename in expected_files:
            path = os.path.join(SQL_DIR, filename)
            if not os.path.isfile(path):
                pytest.skip(f"{filename} not yet created")
            with open(path) as f:
                content = f.read().upper()
            assert object_name in content, (
                f"{filename}: must reference {object_name}"
            )


ALL_SERVICES = [
    "AF_POSTGRES", "AF_REDIS", "AF_API_SERVER",
    "AF_SCHEDULER", "AF_DAG_PROCESSOR", "AF_TRIGGERER", "AF_WORKERS",
]


class TestServiceCreation:
    """07_create_services.sql must CREATE SERVICE for all 7 services."""

    @pytest.mark.parametrize("service", ALL_SERVICES)
    def test_creates_service(self, service):
        path = os.path.join(SQL_DIR, "07_create_services.sql")
        if not os.path.isfile(path):
            pytest.skip("07_create_services.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert service in content, (
            f"07_create_services.sql: must create service {service}"
        )


class TestSuspendResume:
    """Suspend and resume scripts must cover all 7 services."""

    @pytest.mark.parametrize("service", ALL_SERVICES)
    def test_suspend_covers_service(self, service):
        path = os.path.join(SQL_DIR, "09_suspend_all.sql")
        if not os.path.isfile(path):
            pytest.skip("09_suspend_all.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert service in content, (
            f"09_suspend_all.sql: must suspend {service}"
        )

    @pytest.mark.parametrize("service", ALL_SERVICES)
    def test_resume_covers_service(self, service):
        path = os.path.join(SQL_DIR, "10_resume_all.sql")
        if not os.path.isfile(path):
            pytest.skip("10_resume_all.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert service in content, (
            f"10_resume_all.sql: must resume {service}"
        )


class TestComputePools:
    """Compute pool SQL must define all required pools."""

    REQUIRED_POOLS = ["INFRA_POOL", "CORE_POOL", "WORKER_POOL"]

    @pytest.mark.parametrize("pool", REQUIRED_POOLS)
    def test_pool_defined(self, pool):
        path = os.path.join(SQL_DIR, "05_setup_compute_pools.sql")
        if not os.path.isfile(path):
            pytest.skip("05_setup_compute_pools.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert pool in content, (
            f"05_setup_compute_pools.sql: must define {pool}"
        )


class TestValidationScript:
    """08_validate.sql must check service status and endpoints."""

    def test_checks_service_status(self):
        path = os.path.join(SQL_DIR, "08_validate.sql")
        if not os.path.isfile(path):
            pytest.skip("08_validate.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert "SYSTEM$GET_SERVICE_STATUS" in content or "SHOW SERVICES" in content, (
            "08_validate.sql: must check service status"
        )

    def test_checks_endpoints(self):
        path = os.path.join(SQL_DIR, "08_validate.sql")
        if not os.path.isfile(path):
            pytest.skip("08_validate.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert "ENDPOINT" in content, (
            "08_validate.sql: must check service endpoints"
        )


class TestSecretsTemplate:
    """03_setup_secrets.sql.template must define all required secrets."""

    REQUIRED_SECRETS = [
        "AIRFLOW_FERNET_KEY",
        "AIRFLOW_POSTGRES_PWD",
        "AIRFLOW_REDIS_PWD",
        "AIRFLOW_JWT_SECRET",
    ]

    @pytest.mark.parametrize("secret", REQUIRED_SECRETS)
    def test_secret_defined(self, secret):
        path = os.path.join(SQL_DIR, "03_setup_secrets.sql.template")
        if not os.path.isfile(path):
            pytest.skip("03_setup_secrets.sql.template not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert secret in content, (
            f"03_setup_secrets.sql.template: must define secret {secret}"
        )


class TestServiceUpdate:
    """07b_update_services.sql must ALTER SERVICE for all 7 services."""

    @pytest.mark.parametrize("service", ALL_SERVICES)
    def test_alters_service(self, service):
        path = os.path.join(SQL_DIR, "07b_update_services.sql")
        if not os.path.isfile(path):
            pytest.skip("07b_update_services.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert service in content, (
            f"07b_update_services.sql: must ALTER service {service}"
        )

    @pytest.mark.parametrize("service", ALL_SERVICES)
    def test_uses_alter_not_create(self, service):
        """07b must use ALTER SERVICE, never CREATE SERVICE."""
        path = os.path.join(SQL_DIR, "07b_update_services.sql")
        if not os.path.isfile(path):
            pytest.skip("07b_update_services.sql not yet created")
        with open(path) as f:
            content = f.read().upper()
        assert "CREATE SERVICE" not in content, (
            "07b_update_services.sql: must NOT use CREATE SERVICE "
            "(use ALTER SERVICE to preserve ingress URLs)"
        )
        assert "ALTER SERVICE" in content, (
            "07b_update_services.sql: must use ALTER SERVICE"
        )

    def test_references_spec_stage(self):
        """07b must load specs from @SERVICE_SPEC stage."""
        path = os.path.join(SQL_DIR, "07b_update_services.sql")
        if not os.path.isfile(path):
            pytest.skip("07b_update_services.sql not yet created")
        with open(path) as f:
            content = f.read()
        assert "@SERVICE_SPEC" in content, (
            "07b_update_services.sql: must reference @SERVICE_SPEC stage"
        )

    def test_create_and_update_cover_same_services(self):
        """07_create and 07b_update must cover the exact same set of services."""
        create_path = os.path.join(SQL_DIR, "07_create_services.sql")
        update_path = os.path.join(SQL_DIR, "07b_update_services.sql")
        if not os.path.isfile(create_path) or not os.path.isfile(update_path):
            pytest.skip("Both 07 and 07b must exist")
        with open(create_path) as f:
            create_content = f.read().upper()
        with open(update_path) as f:
            update_content = f.read().upper()
        for service in ALL_SERVICES:
            in_create = service in create_content
            in_update = service in update_content
            assert in_create and in_update, (
                f"Service {service} must appear in both 07_create and 07b_update "
                f"(create={in_create}, update={in_update})"
            )

    def test_update_spec_files_match_create(self):
        """SPECIFICATION_FILE refs in 07b must match those in 07."""
        create_path = os.path.join(SQL_DIR, "07_create_services.sql")
        update_path = os.path.join(SQL_DIR, "07b_update_services.sql")
        if not os.path.isfile(create_path) or not os.path.isfile(update_path):
            pytest.skip("Both 07 and 07b must exist")
        with open(create_path) as f:
            create_content = f.read()
        with open(update_path) as f:
            update_content = f.read()
        create_refs = set(re.findall(
            r"SPECIFICATION_FILE\s*=\s*'([^']+)'", create_content, re.IGNORECASE
        ))
        update_refs = set(re.findall(
            r"SPECIFICATION_FILE\s*=\s*'([^']+)'", update_content, re.IGNORECASE
        ))
        assert create_refs == update_refs, (
            f"07_create and 07b_update must reference the same spec files. "
            f"Create: {create_refs}, Update: {update_refs}"
        )
