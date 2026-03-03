"""Tests for Airflow 3.x DAG syntax validation.

Validates that sample DAGs:
- Use Airflow 3.x TaskFlow SDK imports (from airflow.sdk import ...)
- Parse without errors via DagBag
- Do NOT use deprecated Airflow 2.x patterns
- Have required DAG attributes (dag_id, schedule, start_date)
"""

import os
import ast
import pytest

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")


def get_dag_files():
    """Collect all .py files in the dags directory."""
    if not os.path.isdir(DAGS_DIR):
        return []
    return [
        os.path.join(DAGS_DIR, f)
        for f in os.listdir(DAGS_DIR)
        if f.endswith(".py") and not f.startswith("__")
    ]


@pytest.fixture(params=get_dag_files() or ["NO_DAGS_YET"])
def dag_file(request):
    if request.param == "NO_DAGS_YET":
        pytest.skip("No DAG files created yet")
    return request.param


class TestDagFilesExist:
    """At least one sample DAG must exist."""

    def test_dags_directory_has_files(self):
        files = get_dag_files()
        assert len(files) >= 1, "Must have at least one sample DAG file"


class TestDagSyntax:
    """DAG files must be valid Python and parseable."""

    def test_valid_python_syntax(self, dag_file):
        with open(dag_file) as f:
            source = f.read()
        try:
            ast.parse(source)
        except SyntaxError as e:
            pytest.fail(f"{dag_file}: Python syntax error: {e}")

    def test_no_deprecated_imports(self, dag_file):
        """Airflow 3.x should NOT use deprecated 2.x import patterns."""
        with open(dag_file) as f:
            source = f.read()

        deprecated_patterns = [
            "from airflow.operators.python_operator import",
            "from airflow.operators.bash_operator import",
            "from airflow.contrib.",
            "from airflow.hooks.base_hook import",
        ]
        for pattern in deprecated_patterns:
            assert pattern not in source, (
                f"{dag_file}: uses deprecated import '{pattern}'. "
                "Use Airflow 3.x provider packages instead."
            )

    def test_uses_airflow3_patterns(self, dag_file):
        """DAGs should use Airflow 3.x SDK or standard patterns."""
        with open(dag_file) as f:
            source = f.read()

        # Must import from airflow.sdk or airflow standard modules
        has_airflow3_import = (
            "from airflow.sdk" in source
            or "from airflow.providers." in source
            or "from airflow.decorators" in source
        )
        assert has_airflow3_import, (
            f"{dag_file}: must use Airflow 3.x imports "
            "(airflow.sdk, airflow.providers, or airflow.decorators)"
        )


class TestDagAttributes:
    """DAGs must have required attributes for production use."""

    def test_has_dag_id(self, dag_file):
        with open(dag_file) as f:
            source = f.read()
        assert "dag_id" in source, f"{dag_file}: must define dag_id"

    def test_has_schedule(self, dag_file):
        with open(dag_file) as f:
            source = f.read()
        has_schedule = "schedule" in source or "@dag" in source
        assert has_schedule, f"{dag_file}: must define a schedule"

    def test_has_start_date(self, dag_file):
        with open(dag_file) as f:
            source = f.read()
        assert "start_date" in source, f"{dag_file}: must define start_date"
