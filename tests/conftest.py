"""
Shared fixtures for the metaflow-dagster end-to-end test suite.

Design goals (harness engineering):
  - Each test gets its own isolated Metaflow namespace (tmp dir).
  - Compiled Dagster files land in a tmp dir so tests don't interfere.
  - Compilation is done by invoking the real `python flow.py dagster create` CLI.
  - Execution is done via dagster.execute_job() — real Dagster, no mocks.
"""

import importlib
import importlib.util
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

FLOWS_DIR = Path(__file__).parent / "flows"
METAFLOW_HOME = None  # filled by fixture


@pytest.fixture(scope="session", autouse=True)
def install_extension():
    """Ensure metaflow-dagster extension is on sys.path for the test session."""
    repo_root = Path(__file__).parent.parent
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-e", str(repo_root), "--quiet"],
        check=True,
    )


@pytest.fixture
def metaflow_home(tmp_path):
    """Isolated Metaflow home + local datastore root per test."""
    mf_home = tmp_path / "metaflow_home"
    mf_home.mkdir()
    ds_root = tmp_path / "metaflow_ds"
    ds_root.mkdir()
    return mf_home, ds_root


@pytest.fixture
def dagster_file(tmp_path):
    """Return a factory: compile flow → Dagster file and return its path."""

    def _compile(flow_path: Path, metaflow_home: Path, ds_root: Path, extra_args=None) -> Path:
        out = tmp_path / (flow_path.stem + "_dagster.py")
        env = {
            **os.environ,
            "METAFLOW_HOME": str(metaflow_home),
            "METAFLOW_DEFAULT_DATASTORE": "local",
            "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(ds_root),
        }
        cmd = [
            sys.executable,
            str(flow_path),
            "--no-pylint",
            "--datastore=local",
            f"--datastore-root={ds_root}",
            "--metadata=local",
            "dagster",
            "create",
            str(out),
        ]
        if extra_args:
            cmd.extend(extra_args)
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            raise AssertionError(
                f"dagster create failed for {flow_path.name}:\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )
        assert out.exists(), f"Expected output file {out} was not created"
        return out

    return _compile


@pytest.fixture
def load_dagster_defs():
    """Return a factory: load a generated Dagster file as a module."""

    def _load(defs_path: Path) -> types.ModuleType:
        spec = importlib.util.spec_from_file_location("_defs", str(defs_path))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    return _load


@pytest.fixture
def run_job():
    """Return a factory: execute a Dagster job and assert success."""
    from dagster import execute_job, reconstructable

    def _run(job_def, run_config=None, resources=None) -> object:
        result = job_def.execute_in_process(run_config=run_config or {})
        assert result.success, (
            f"Job {job_def.name!r} failed.\n"
            + "\n".join(str(e) for e in result.all_node_events if "FAILURE" in str(e.event_type))
        )
        return result

    return _run
