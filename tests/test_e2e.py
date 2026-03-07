"""
End-to-end tests for the metaflow-dagster integration.

Each execution test:
  1. Compiles a Metaflow flow → Dagster definitions file (real CLI invocation).
  2. Executes the job via `dagster job execute` (out-of-process, real orchestrator).
  3. Verifies Metaflow artifact correctness by reading the local datastore.

Compilation tests (TestCompilation) are fast — they only check generated code,
no execution.  Execution tests are marked @pytest.mark.e2e.

Harness structure:
  - _compile()          → invoke `python flow.py dagster create`
  - _run_job_cli()      → invoke `dagster job execute -f ... -j ...`
  - _latest_run_id()    → find the most recent run in the local datastore
  - _read_artifact()    → read a gzip+pickle artifact directly from disk
"""

import ast
import gzip
import importlib
import importlib.util
import json
import os
import pickle
import re
import subprocess
import sys
import types
from pathlib import Path

import pytest

FLOWS_DIR = Path(__file__).parent / "flows"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

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


def _run_job_cli(job_name: str, out_path: Path, ds_root: Path, dagster_home: Path,
                 run_config: dict | None = None) -> None:
    """Execute a compiled Dagster definitions file via `dagster job execute`.

    Uses a real out-of-process Dagster execution engine backed by a temporary
    SQLite instance in dagster_home.  Does NOT require a running webserver or
    daemon.
    """
    dagster_home.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "DAGSTER_HOME": str(dagster_home),
        "METAFLOW_DEFAULT_DATASTORE": "local",
        "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(ds_root),
        "METAFLOW_DEFAULT_METADATA": "local",
    }
    cmd = [
        sys.executable, "-m", "dagster", "job", "execute",
        "-f", str(out_path),
        "-j", job_name,
    ]
    if run_config:
        config_file = dagster_home / "run_config.json"
        config_file.write_text(json.dumps(run_config))
        cmd += ["-c", str(config_file)]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise AssertionError(
            f"dagster job execute failed (job={job_name!r}):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _latest_run_id(ds_root: Path, flow_name: str) -> str:
    """Return the most recently written run_id from the local datastore."""
    flow_dir = ds_root / flow_name
    if not flow_dir.exists():
        raise AssertionError(f"No runs found for {flow_name!r} under {ds_root}")
    runs = [d for d in flow_dir.iterdir() if d.is_dir() and d.name != "data"]
    if not runs:
        raise AssertionError(f"No run directories found in {flow_dir}")
    return sorted(runs, key=lambda d: d.stat().st_mtime)[-1].name


def _load_module(path: Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("_dagster_defs", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _read_artifact(ds_root: Path, flow_name: str, run_id: str, step: str, artifact: str,
                   task_id: str = "1"):
    """Read a Metaflow artifact directly from the local datastore.

    Layout:
      <ds_root>/<flow>/<run>/<step>/<task>/0.data.json  → name→sha mapping
      <ds_root>/<flow>/data/<sha[:2]>/<sha>             → gzip+pickle blob
    """
    data_json = ds_root / flow_name / run_id / step / task_id / "0.data.json"
    if not data_json.exists():
        raise AssertionError(
            f"Datastore file not found: {data_json}\n"
            f"Available runs: {list((ds_root / flow_name).iterdir()) if (ds_root / flow_name).exists() else 'none'}"
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


# ──────────────────────────────────────────────────────────────────────────────
# Compilation correctness tests (fast, no execution)
# ──────────────────────────────────────────────────────────────────────────────

class TestCompilation:

    def test_linear_compiles(self, tmp_path):
        out = tmp_path / "linear_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "linear_flow.py", out, ds)
        code = out.read_text()

        assert "DO NOT EDIT" in code
        assert "LinearFlow" in code
        assert "def op_start(" in code
        assert "def op_process(" in code
        assert "def op_end(" in code
        assert "@job" in code
        assert "def LinearFlow(" in code
        assert "defs = Definitions(" in code

    def test_branching_compiles(self, tmp_path):
        out = tmp_path / "branching_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "branching_flow.py", out, ds)
        code = out.read_text()

        assert "def op_start(" in code
        assert "def op_branch_a(" in code
        assert "def op_branch_b(" in code
        assert "def op_join(" in code
        assert "def op_end(" in code
        assert '"branch_a": Out(str)' in code
        assert '"branch_b": Out(str)' in code

    def test_foreach_compiles(self, tmp_path):
        out = tmp_path / "foreach_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "foreach_flow.py", out, ds)
        code = out.read_text()

        assert "DynamicOut" in code
        assert "DynamicOutput" in code
        assert "def op_process_item(" in code
        assert ".map(" in code
        assert ".collect()" in code

    def test_parametrized_compiles(self, tmp_path):
        out = tmp_path / "param_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "parametrized_flow.py", out, ds)
        code = out.read_text()

        assert "class ParametrizedFlowConfig(Config):" in code
        assert "greeting" in code
        assert "count" in code
        assert "ParametrizedFlowConfig" in code

    def test_custom_job_name(self, tmp_path):
        out = tmp_path / "named_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(
            FLOWS_DIR / "linear_flow.py", out, ds,
            extra_args=["--name", "my_custom_job"],
        )
        code = out.read_text()
        assert "def my_custom_job(" in code

    def test_generated_file_is_syntactically_valid(self, tmp_path):
        for flow in FLOWS_DIR.glob("*.py"):
            out = tmp_path / f"{flow.stem}_dagster.py"
            ds = tmp_path / f"ds_{flow.stem}"
            ds.mkdir()
            _compile(flow, out, ds)
            try:
                ast.parse(out.read_text())
            except SyntaxError as e:
                pytest.fail(f"Generated file for {flow.name} has syntax error: {e}")

    def test_tags_embedded(self, tmp_path):
        out = tmp_path / "tagged_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(
            FLOWS_DIR / "linear_flow.py", out, ds,
            extra_args=["--tag", "env:test", "--tag", "version:1"],
        )
        code = out.read_text()
        assert "env:test" in code
        assert "version:1" in code


# ──────────────────────────────────────────────────────────────────────────────
# End-to-end execution tests (run via `dagster job execute`)
#
# Each class uses a class-scoped fixture that compiles once and runs the job
# once, sharing the compiled definitions file and datastore across all test
# methods in that class.
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="class")
def _linear_env(tmp_path_factory):
    base = tmp_path_factory.mktemp("linear")
    out = base / "linear_dagster.py"
    ds = base / "ds"
    ds.mkdir()
    _compile(FLOWS_DIR / "linear_flow.py", out, ds)
    _run_job_cli("LinearFlow", out, ds, base / "dagster_home")
    return out, ds


@pytest.mark.e2e
class TestLinearFlow:

    def test_executes_successfully(self, _linear_env):
        pass  # execution in fixture; success means no exception was raised

    def test_all_steps_ran(self, _linear_env):
        out, ds = _linear_env
        run_id = _latest_run_id(ds, "LinearFlow")
        assert (ds / "LinearFlow" / run_id / "start" / "1").exists()
        assert (ds / "LinearFlow" / run_id / "process" / "1").exists()
        assert (ds / "LinearFlow" / run_id / "end" / "1").exists()

    def test_metaflow_artifacts_persisted(self, _linear_env):
        out, ds = _linear_env
        run_id = _latest_run_id(ds, "LinearFlow")
        value = _read_artifact(ds, "LinearFlow", run_id, "process", "result")
        assert value == "hello from start -> process"


@pytest.fixture(scope="class")
def _branching_env(tmp_path_factory):
    base = tmp_path_factory.mktemp("branching")
    out = base / "branching_dagster.py"
    ds = base / "ds"
    ds.mkdir()
    _compile(FLOWS_DIR / "branching_flow.py", out, ds)
    _run_job_cli("BranchingFlow", out, ds, base / "dagster_home")
    return out, ds


@pytest.mark.e2e
class TestBranchingFlow:

    def test_executes_successfully(self, _branching_env):
        pass  # execution in fixture; success means no exception was raised

    def test_both_branches_ran(self, _branching_env):
        out, ds = _branching_env
        run_id = _latest_run_id(ds, "BranchingFlow")
        assert (ds / "BranchingFlow" / run_id / "branch_a" / "1").exists()
        assert (ds / "BranchingFlow" / run_id / "branch_b" / "1").exists()
        assert (ds / "BranchingFlow" / run_id / "join" / "1").exists()

    def test_join_artifacts_correct(self, _branching_env):
        out, ds = _branching_env
        run_id = _latest_run_id(ds, "BranchingFlow")
        assert _read_artifact(ds, "BranchingFlow", run_id, "join", "merged_a") == 20
        assert _read_artifact(ds, "BranchingFlow", run_id, "join", "merged_b") == 15


@pytest.fixture(scope="class")
def _foreach_env(tmp_path_factory):
    base = tmp_path_factory.mktemp("foreach")
    out = base / "foreach_dagster.py"
    ds = base / "ds"
    ds.mkdir()
    _compile(FLOWS_DIR / "foreach_flow.py", out, ds)
    _run_job_cli("ForeachFlow", out, ds, base / "dagster_home")
    return out, ds


@pytest.mark.e2e
class TestForeachFlow:

    def test_executes_successfully(self, _foreach_env):
        pass  # execution in fixture; success means no exception was raised

    def test_all_items_processed(self, _foreach_env):
        out, ds = _foreach_env
        run_id = _latest_run_id(ds, "ForeachFlow")
        process_item_dir = ds / "ForeachFlow" / run_id / "process_item"
        tasks = [d for d in process_item_dir.iterdir() if d.is_dir()]
        assert len(tasks) >= 3, f"Expected >=3 process_item tasks, found {len(tasks)}"

    def test_foreach_artifacts_correct(self, _foreach_env):
        out, ds = _foreach_env
        run_id = _latest_run_id(ds, "ForeachFlow")
        results = _read_artifact(ds, "ForeachFlow", run_id, "join", "results")
        assert sorted(results) == ["APPLE", "BANANA", "CHERRY"]


@pytest.fixture(scope="class")
def _param_env_default(tmp_path_factory):
    base = tmp_path_factory.mktemp("param_default")
    out = base / "param_dagster.py"
    ds = base / "ds"
    ds.mkdir()
    _compile(FLOWS_DIR / "parametrized_flow.py", out, ds)
    _run_job_cli("ParametrizedFlow", out, ds, base / "dagster_home")
    return out, ds


@pytest.fixture(scope="class")
def _param_env_custom(tmp_path_factory):
    base = tmp_path_factory.mktemp("param_custom")
    out = base / "param_dagster.py"
    ds = base / "ds"
    ds.mkdir()
    _compile(FLOWS_DIR / "parametrized_flow.py", out, ds)
    run_config = {
        "ops": {"op_start": {"config": {"greeting": "Hi", "count": 5}}}
    }
    _run_job_cli("ParametrizedFlow", out, ds, base / "dagster_home", run_config=run_config)
    return out, ds


@pytest.mark.e2e
class TestParametrizedFlow:

    def test_default_parameters(self, _param_env_default):
        pass  # execution in fixture; success means no exception was raised

    def test_custom_parameters(self, _param_env_custom):
        out, ds = _param_env_custom
        run_id = _latest_run_id(ds, "ParametrizedFlow")
        messages = _read_artifact(ds, "ParametrizedFlow", run_id, "start", "messages")
        assert len(messages) == 5
        assert all(m.startswith("Hi #") for m in messages)

    def test_parameter_config_schema(self, _param_env_default):
        out, ds = _param_env_default
        mod = _load_module(out)
        config_cls = mod.ParametrizedFlowConfig
        fields = config_cls.__fields__ if hasattr(config_cls, "__fields__") else config_cls.model_fields
        assert "greeting" in fields
        assert "count" in fields


# ──────────────────────────────────────────────────────────────────────────────
# Decorator feature tests (@retry, @timeout, @environment, @project, --workflow-timeout)
# ──────────────────────────────────────────────────────────────────────────────

class TestDecoratorCompilation:

    def test_retry_generates_retry_policy(self, tmp_path):
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()
        assert "RetryPolicy" in code
        assert "max_retries=3" in code

    def test_retry_delay_generates_delay(self, tmp_path):
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()
        assert "delay=120" in code

    def test_timeout_generates_op_tag(self, tmp_path):
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()
        assert "dagster/op_execution_timeout" in code
        assert '"120"' in code

    def test_environment_vars_in_extra_env(self, tmp_path):
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()
        assert "MY_VAR" in code
        assert "hello" in code
        assert "OTHER" in code

    def test_retry_count_uses_context_retry_number(self, tmp_path):
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()
        assert "retry_count=context.retry_number" in code

    def test_workflow_timeout_sets_job_tag(self, tmp_path):
        out = tmp_path / "timeout_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "linear_flow.py", out, ds, extra_args=["--workflow-timeout", "3600"])
        code = out.read_text()
        assert "dagster/max_runtime" in code
        assert "3600" in code

    def test_no_retry_no_retry_policy_on_ops(self, tmp_path):
        out = tmp_path / "linear_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "linear_flow.py", out, ds)
        code = out.read_text()
        assert "retry_policy=" not in code


@pytest.fixture(scope="class")
def _retry_env(tmp_path_factory):
    base = tmp_path_factory.mktemp("retry")
    out = base / "retry_dagster.py"
    ds = base / "ds"
    ds.mkdir()
    _compile(FLOWS_DIR / "retry_flow.py", out, ds)
    _run_job_cli("RetryFlow", out, ds, base / "dagster_home")
    return out, ds


@pytest.mark.e2e
class TestDecoratorExecution:

    def test_retry_flow_executes(self, _retry_env):
        out, ds = _retry_env
        run_id = _latest_run_id(ds, "RetryFlow")
        result_val = _read_artifact(ds, "RetryFlow", run_id, "process", "result")
        assert result_val == 2
