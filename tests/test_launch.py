"""
Launch-path tests for the metaflow-dagster integration.

These tests exercise the full Dagster run lifecycle via execute_job() with a
real DagsterInstance backed by SQLite.  This covers the path that differs from
`dagster job execute`:

  compile flow → execute_job(recon_job, instance=instance)
               → run created + persisted in Dagster SQLite DB
               → DefaultRunCoordinator.submit_run()
               → SyncInMemoryRunLauncher.launch_run()  (no daemon required)
               → Metaflow steps execute as subprocesses
               → Metaflow run + artifacts appear in local datastore
               → Dagster DB reflects SUCCESS status

No webserver, daemon, or GRPC server is required.  The SyncInMemoryRunLauncher
executes runs synchronously in the same process, bypassing the GRPC layer while
still exercising the full run-coordinator → run-launcher → execution path.

To configure: set DAGSTER_HOME to a directory containing::

    storage:
      sqlite:
        base_dir: <path>
    run_launcher:
      module: dagster._core.launcher.sync_in_memory_run_launcher
      class: SyncInMemoryRunLauncher
    run_coordinator:
      module: dagster._core.run_coordinator.default_run_coordinator
      class: DefaultRunCoordinator

The fixture ``dagster_instance`` below writes this file automatically.
"""

from __future__ import annotations

import gzip
import json
import os
import pickle
import subprocess
import sys
from pathlib import Path

import pytest

FLOWS_DIR = Path(__file__).parent / "flows"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compile(flow_path: Path, out_path: Path, ds_root: Path, extra_args=None) -> None:
    env = {
        **os.environ,
        "METAFLOW_DEFAULT_DATASTORE": "local",
        "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(ds_root),
    }
    cmd = [
        sys.executable, str(flow_path),
        "--no-pylint",
        "--datastore=local",
        f"--datastore-root={ds_root}",
        "--metadata=local",
        "dagster", "create", str(out_path),
    ]
    if extra_args:
        cmd.extend(extra_args)
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise AssertionError(
            f"dagster create failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _latest_run_id(ds_root: Path, flow_name: str) -> str:
    flow_dir = ds_root / flow_name
    if not flow_dir.exists():
        raise AssertionError(f"No runs found for {flow_name!r} under {ds_root}")
    runs = [d for d in flow_dir.iterdir() if d.is_dir() and d.name not in ("data", "_meta")]
    if not runs:
        raise AssertionError(f"No run directories found in {flow_dir}")
    return sorted(runs, key=lambda d: d.stat().st_mtime)[-1].name


def _read_artifact(ds_root: Path, flow_name: str, run_id: str, step: str, artifact: str,
                   task_id: str = "1"):
    data_json = ds_root / flow_name / run_id / step / task_id / "0.data.json"
    if not data_json.exists():
        raise AssertionError(
            f"Datastore file not found: {data_json}\n"
            f"Available: {list((ds_root / flow_name).iterdir())}"
        )
    data_map = json.loads(data_json.read_text())
    objects = data_map.get("objects", {})
    if artifact not in objects:
        raise AssertionError(
            f"Artifact {artifact!r} not in step {step!r}. Available: {list(objects)}"
        )
    sha = objects[artifact]
    data_file = ds_root / flow_name / "data" / sha[:2] / sha
    with gzip.open(data_file) as f:
        return pickle.load(f)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dagster_instance(tmp_path_factory):
    """Yield a DagsterInstance backed by a temporary SQLite store.

    Uses SyncInMemoryRunLauncher (no GRPC, no daemon) and DefaultRunCoordinator
    (no daemon queue).  Runs are persisted to SQLite so we can verify status
    after execution.
    """
    dagster_home = tmp_path_factory.mktemp("dagster_home_launch")
    storage_dir = dagster_home / "storage"
    storage_dir.mkdir()

    dagster_yaml = dagster_home / "dagster.yaml"
    dagster_yaml.write_text(
        f"""\
storage:
  sqlite:
    base_dir: {storage_dir}
run_launcher:
  module: dagster._core.launcher.sync_in_memory_run_launcher
  class: SyncInMemoryRunLauncher
run_coordinator:
  module: dagster._core.run_coordinator.default_run_coordinator
  class: DefaultRunCoordinator
telemetry:
  enabled: false
"""
    )

    old_home = os.environ.get("DAGSTER_HOME")
    os.environ["DAGSTER_HOME"] = str(dagster_home)
    try:
        from dagster import DagsterInstance
        with DagsterInstance.get() as instance:
            yield instance
    finally:
        if old_home is None:
            os.environ.pop("DAGSTER_HOME", None)
        else:
            os.environ["DAGSTER_HOME"] = old_home


def _launch_job(defs_file: Path, job_name: str, ds_root: Path, instance) -> str:
    """Compile a definitions file, launch via execute_job, return Dagster run_id."""
    from dagster._core.definitions.reconstruct import ReconstructableJob
    from dagster._core.execution.api import execute_job

    recon_job = ReconstructableJob.for_file(str(defs_file), job_name)

    env_patch = {
        "METAFLOW_DEFAULT_DATASTORE": "local",
        "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(ds_root),
        "METAFLOW_DEFAULT_METADATA": "local",
    }
    old_vals = {k: os.environ.get(k) for k in env_patch}
    try:
        os.environ.update(env_patch)
        result = execute_job(recon_job, instance=instance)
    finally:
        for k, v in old_vals.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    return result.run_id, result.success


# ---------------------------------------------------------------------------
# Linear flow — full launch path
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _linear_launch_env(tmp_path_factory, dagster_instance):
    base = tmp_path_factory.mktemp("linear_launch")
    ds = base / "ds"
    ds.mkdir()
    defs = base / "linear_dagster.py"
    _compile(FLOWS_DIR / "linear_flow.py", defs, ds)
    run_id, success = _launch_job(defs, "LinearFlow", ds, dagster_instance)
    return run_id, success, ds, dagster_instance


@pytest.mark.e2e
class TestLinearFlowLaunch:
    """Verify the full Dagster run-lifecycle path for a linear flow."""

    def test_launch_succeeds(self, _linear_launch_env):
        run_id, success, ds, instance = _linear_launch_env
        assert success, f"execute_job returned failure for run {run_id}"

    def test_run_persisted_in_dagster_db(self, _linear_launch_env):
        """Run must be recorded in the Dagster SQLite DB with SUCCESS status."""
        run_id, success, ds, instance = _linear_launch_env
        run = instance.get_run_by_id(run_id)
        assert run is not None, f"Run {run_id} not found in Dagster DB"
        assert run.status.value == "SUCCESS", (
            f"Expected SUCCESS, got {run.status.value}"
        )

    def test_metaflow_run_appears_in_local_metadata(self, _linear_launch_env):
        """Metaflow run directory must exist in the local datastore."""
        run_id, success, ds, instance = _linear_launch_env
        mf_run_id = _latest_run_id(ds, "LinearFlow")
        assert mf_run_id.startswith("dagster-"), (
            f"Expected Metaflow run ID to start with 'dagster-', got {mf_run_id!r}"
        )

    def test_all_steps_ran(self, _linear_launch_env):
        run_id, success, ds, instance = _linear_launch_env
        mf_run_id = _latest_run_id(ds, "LinearFlow")
        assert (ds / "LinearFlow" / mf_run_id / "start" / "1").exists()
        assert (ds / "LinearFlow" / mf_run_id / "process" / "1").exists()
        assert (ds / "LinearFlow" / mf_run_id / "end" / "1").exists()

    def test_metaflow_artifact_correct(self, _linear_launch_env):
        run_id, success, ds, instance = _linear_launch_env
        mf_run_id = _latest_run_id(ds, "LinearFlow")
        value = _read_artifact(ds, "LinearFlow", mf_run_id, "process", "result")
        assert value == "hello from start -> process"


# ---------------------------------------------------------------------------
# Branching flow — launch path verifies parallel branches complete
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _branching_launch_env(tmp_path_factory, dagster_instance):
    base = tmp_path_factory.mktemp("branching_launch")
    ds = base / "ds"
    ds.mkdir()
    defs = base / "branching_dagster.py"
    _compile(FLOWS_DIR / "branching_flow.py", defs, ds)
    run_id, success = _launch_job(defs, "BranchingFlow", ds, dagster_instance)
    return run_id, success, ds, dagster_instance


@pytest.mark.e2e
class TestBranchingFlowLaunch:

    def test_launch_succeeds(self, _branching_launch_env):
        run_id, success, ds, instance = _branching_launch_env
        assert success, f"execute_job returned failure for run {run_id}"

    def test_run_persisted_in_dagster_db(self, _branching_launch_env):
        run_id, success, ds, instance = _branching_launch_env
        run = instance.get_run_by_id(run_id)
        assert run is not None
        assert run.status.value == "SUCCESS"

    def test_both_branches_ran(self, _branching_launch_env):
        run_id, success, ds, instance = _branching_launch_env
        mf_run_id = _latest_run_id(ds, "BranchingFlow")
        assert (ds / "BranchingFlow" / mf_run_id / "branch_a" / "1").exists()
        assert (ds / "BranchingFlow" / mf_run_id / "branch_b" / "1").exists()
        assert (ds / "BranchingFlow" / mf_run_id / "join" / "1").exists()
