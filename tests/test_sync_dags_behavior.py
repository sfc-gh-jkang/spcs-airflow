"""Behavioral tests for sync_dags.sh.

Instead of just checking that the script *contains* certain strings, these
tests actually execute sync_dags.sh against a temporary dags/ directory
with a fake `snow` command, then verify which PUT commands were generated.

This catches real bugs like:
- Subdirectories not being uploaded
- __pycache__ leaking onto the stage
- Wrong stage paths for nested files
- File count being wrong in output
- Missing files not producing warnings
"""

import os
import stat
import subprocess
import textwrap

import pytest

SCRIPT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "scripts", "sync_dags.sh"
)


@pytest.fixture
def fake_project(tmp_path):
    """Build a realistic fake project tree with a mock `snow` CLI.

    Layout:
        tmp_path/
        ├── dags/
        │   ├── dag_a.py
        │   ├── dag_b.py
        │   └── utils/
        │       ├── __init__.py
        │       └── snowflake_conn.py
        ├── scripts/
        │   └── sync_dags.sh          (symlink to real script)
        └── bin/
            └── snow                   (fake — logs every call to snow.log)
    """
    # -- dags/ ----------------------------------------------------------------
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "dag_a.py").write_text("# dag a\n")
    (dags / "dag_b.py").write_text("# dag b\n")

    utils = dags / "utils"
    utils.mkdir()
    (utils / "__init__.py").write_text("# init\n")
    (utils / "snowflake_conn.py").write_text("# conn helper\n")

    # -- scripts/ (symlink so SCRIPT_DIR/../dags resolves to our tmp tree) ----
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    os.symlink(os.path.abspath(SCRIPT_PATH), str(scripts / "sync_dags.sh"))

    # -- bin/snow (fake CLI that logs every invocation) -----------------------
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    snow_log = tmp_path / "snow.log"
    fake_snow = bin_dir / "snow"
    fake_snow.write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        echo "$@" >> "{snow_log}"
    """))
    fake_snow.chmod(fake_snow.stat().st_mode | stat.S_IEXEC)

    return tmp_path, snow_log


def _run_sync(fake_project, extra_args=None):
    """Run sync_dags.sh inside the fake project and return (stdout, log_lines)."""
    tmp_path, snow_log = fake_project
    script = str(tmp_path / "scripts" / "sync_dags.sh")

    env = os.environ.copy()
    # Prepend our fake bin/ so `snow` resolves to our logger
    env["PATH"] = str(tmp_path / "bin") + ":" + env.get("PATH", "")

    cmd = ["bash", script, "--connection", "test_conn"]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
    )

    log_lines = []
    if snow_log.exists():
        log_lines = [l.strip() for l in snow_log.read_text().splitlines() if l.strip()]

    return result, log_lines


# ---------------------------------------------------------------------------
# Tests: default sync (no file args)
# ---------------------------------------------------------------------------


class TestSyncAllFiles:
    """When called with no file arguments, sync_dags.sh should upload
    every .py file in dags/ plus every .py in subdirectories."""

    def test_uploads_top_level_py_files(self, fake_project):
        result, log_lines = _run_sync(fake_project)
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"

        put_queries = [l for l in log_lines if "PUT" in l]
        filenames_uploaded = [l for l in put_queries]

        # dag_a.py and dag_b.py should be uploaded to the stage root
        assert any("dag_a.py" in q for q in filenames_uploaded), (
            f"dag_a.py not found in PUT commands: {filenames_uploaded}"
        )
        assert any("dag_b.py" in q for q in filenames_uploaded), (
            f"dag_b.py not found in PUT commands: {filenames_uploaded}"
        )

    def test_uploads_subdirectory_files(self, fake_project):
        result, log_lines = _run_sync(fake_project)
        assert result.returncode == 0, f"Script failed:\n{result.stderr}"

        put_queries = [l for l in log_lines if "PUT" in l]

        # utils/__init__.py and utils/snowflake_conn.py should be uploaded
        assert any("__init__.py" in q for q in put_queries), (
            f"utils/__init__.py not found in PUT commands: {put_queries}"
        )
        assert any("snowflake_conn.py" in q for q in put_queries), (
            f"utils/snowflake_conn.py not found in PUT commands: {put_queries}"
        )

    def test_subdirectory_files_target_stage_subpath(self, fake_project):
        """Files in utils/ must be PUT to @...AIRFLOW_DAGS/utils, not the stage root."""
        result, log_lines = _run_sync(fake_project)
        assert result.returncode == 0

        utils_puts = [l for l in log_lines if "snowflake_conn.py" in l]
        assert len(utils_puts) == 1
        assert "AIRFLOW_DAGS/utils" in utils_puts[0], (
            f"utils/ files should target AIRFLOW_DAGS/utils on the stage, got: {utils_puts[0]}"
        )

    def test_top_level_files_target_stage_root(self, fake_project):
        """Top-level .py files must be PUT to @...AIRFLOW_DAGS (no subpath)."""
        result, log_lines = _run_sync(fake_project)
        assert result.returncode == 0

        dag_a_puts = [l for l in log_lines if "dag_a.py" in l]
        assert len(dag_a_puts) == 1
        # Should target AIRFLOW_DAGS but NOT AIRFLOW_DAGS/<something>
        assert "AIRFLOW_DAGS " in dag_a_puts[0] or "AIRFLOW_DAGS AUTO" in dag_a_puts[0], (
            f"Top-level files should target stage root, got: {dag_a_puts[0]}"
        )

    def test_correct_file_count_in_output(self, fake_project):
        """Output should report the correct total count (top-level + subdirs)."""
        result, _ = _run_sync(fake_project)
        assert result.returncode == 0
        # 2 top-level + 2 in utils/ = 4
        assert "4 file(s)" in result.stdout, (
            f"Expected '4 file(s)' in output, got:\n{result.stdout}"
        )

    def test_uses_correct_connection(self, fake_project):
        """All snow commands should use the --connection flag value."""
        _, log_lines = _run_sync(fake_project)
        for line in log_lines:
            assert "test_conn" in line, (
                f"Expected connection 'test_conn' in: {line}"
            )

    def test_auto_compress_false(self, fake_project):
        """Every PUT must include AUTO_COMPRESS=FALSE."""
        _, log_lines = _run_sync(fake_project)
        put_lines = [l for l in log_lines if "PUT" in l]
        assert len(put_lines) > 0, "No PUT commands generated"
        for line in put_lines:
            assert "AUTO_COMPRESS=FALSE" in line, (
                f"Missing AUTO_COMPRESS=FALSE in: {line}"
            )

    def test_overwrite_true(self, fake_project):
        """Every PUT must include OVERWRITE=TRUE."""
        _, log_lines = _run_sync(fake_project)
        put_lines = [l for l in log_lines if "PUT" in l]
        for line in put_lines:
            assert "OVERWRITE=TRUE" in line, (
                f"Missing OVERWRITE=TRUE in: {line}"
            )


# ---------------------------------------------------------------------------
# Tests: __pycache__ exclusion
# ---------------------------------------------------------------------------


class TestPycacheExclusion:
    """__pycache__ directories must never be uploaded."""

    def test_pycache_not_uploaded(self, fake_project):
        tmp_path, _ = fake_project
        pycache = tmp_path / "dags" / "__pycache__"
        pycache.mkdir()
        (pycache / "dag_a.cpython-312.pyc").write_text("bytecode")
        # Also a .py in __pycache__ (shouldn't happen but test defensively)
        (pycache / "leaked.py").write_text("# should not be uploaded")

        result, log_lines = _run_sync(fake_project)
        assert result.returncode == 0

        all_queries = " ".join(log_lines)
        assert "__pycache__" not in all_queries, (
            f"__pycache__ files were uploaded: {log_lines}"
        )
        assert "leaked.py" not in all_queries

    def test_nested_pycache_not_uploaded(self, fake_project):
        """__pycache__ inside subdirectories should also be skipped."""
        tmp_path, _ = fake_project
        nested_cache = tmp_path / "dags" / "utils" / "__pycache__"
        nested_cache.mkdir()
        (nested_cache / "snowflake_conn.cpython-312.pyc").write_text("bytecode")

        result, log_lines = _run_sync(fake_project)
        assert result.returncode == 0

        # The script only iterates one level of subdirs, so nested __pycache__
        # inside utils/ won't be iterated (it's not a direct child of dags/).
        # Just verify no .pyc content leaked.
        all_queries = " ".join(log_lines)
        assert ".pyc" not in all_queries


# ---------------------------------------------------------------------------
# Tests: specific file arguments
# ---------------------------------------------------------------------------


class TestSpecificFiles:
    """When file arguments are given, only those files should be uploaded."""

    def test_syncs_only_named_file(self, fake_project):
        result, log_lines = _run_sync(fake_project, extra_args=["dag_a.py"])
        assert result.returncode == 0

        put_lines = [l for l in log_lines if "PUT" in l]
        assert len(put_lines) == 1, (
            f"Expected exactly 1 PUT, got {len(put_lines)}: {put_lines}"
        )
        assert "dag_a.py" in put_lines[0]

    def test_syncs_multiple_named_files(self, fake_project):
        result, log_lines = _run_sync(fake_project, extra_args=["dag_a.py", "dag_b.py"])
        assert result.returncode == 0

        put_lines = [l for l in log_lines if "PUT" in l]
        assert len(put_lines) == 2
        uploaded = " ".join(put_lines)
        assert "dag_a.py" in uploaded
        assert "dag_b.py" in uploaded

    def test_warns_on_missing_file(self, fake_project):
        result, log_lines = _run_sync(fake_project, extra_args=["nonexistent.py"])
        # Script should still exit 0 (warnings, not errors)
        assert result.returncode == 0
        assert "WARNING" in result.stdout
        assert "nonexistent.py" in result.stdout
        # No PUT should have been issued
        put_lines = [l for l in log_lines if "PUT" in l]
        assert len(put_lines) == 0

    def test_specific_file_count_in_output(self, fake_project):
        result, _ = _run_sync(fake_project, extra_args=["dag_a.py"])
        assert result.returncode == 0
        assert "1 DAG file(s)" in result.stdout


# ---------------------------------------------------------------------------
# Tests: empty dags directory
# ---------------------------------------------------------------------------


class TestEmptyDagsDir:
    """Script should fail gracefully when dags/ has no .py files."""

    def test_exits_nonzero_with_empty_dags(self, tmp_path):
        # Build a minimal project with empty dags/
        dags = tmp_path / "dags"
        dags.mkdir()
        scripts = tmp_path / "scripts"
        scripts.mkdir()
        os.symlink(os.path.abspath(SCRIPT_PATH), str(scripts / "sync_dags.sh"))

        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        fake_snow = bin_dir / "snow"
        fake_snow.write_text("#!/usr/bin/env bash\necho \"$@\"\n")
        fake_snow.chmod(fake_snow.stat().st_mode | stat.S_IEXEC)

        env = os.environ.copy()
        env["PATH"] = str(bin_dir) + ":" + env.get("PATH", "")

        result = subprocess.run(
            ["bash", str(scripts / "sync_dags.sh"), "--connection", "test"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 1
        assert "No DAG files found" in result.stdout


# ---------------------------------------------------------------------------
# Tests: output format
# ---------------------------------------------------------------------------


class TestOutputMessages:
    """Verify user-facing output messages are helpful."""

    def test_shows_done_message(self, fake_project):
        result, _ = _run_sync(fake_project)
        assert "Done" in result.stdout

    def test_shows_monitoring_hint(self, fake_project):
        result, _ = _run_sync(fake_project)
        assert "GET_SERVICE_LOGS" in result.stdout

    def test_shows_put_for_each_file(self, fake_project):
        result, _ = _run_sync(fake_project)
        # Should list each file being PUT
        assert "PUT dag_a.py" in result.stdout
        assert "PUT dag_b.py" in result.stdout
        assert "PUT utils/__init__.py" in result.stdout
        assert "PUT utils/snowflake_conn.py" in result.stdout
