"""
End-to-end tests for the metaflow-dagster integration.

Each test:
  1. Compiles a Metaflow flow → Dagster definitions file (real CLI invocation).
  2. Loads the file as a Python module (real import).
  3. Executes the Dagster job (real execute_in_process, no mocks).
  4. Verifies both Dagster success AND Metaflow artifact correctness.

Harness structure:
  - compile_and_load()  → shared helper used by all tests
  - test_linear_*       → linear flow tests
  - test_branching_*    → split/join tests
  - test_foreach_*      → foreach/dynamic tests
  - test_parametrized_* → parameter passing tests
  - test_compile_*      → compilation correctness tests (no execution)
"""

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


def _load_module(path: Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("_dagster_defs", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run_job(job, run_config=None):
    result = job.execute_in_process(run_config=run_config or {})
    assert result.success, (
        "Job failed. Events:\n"
        + "\n".join(
            f"  {e.event_type}: {e.message}"
            for e in result.all_node_events
            if e.event_type.value in ("STEP_FAILURE", "RUN_FAILURE", "PIPELINE_FAILURE")
        )
    )
    return result


def _read_artifact(ds_root: Path, flow_name: str, run_id: str, step: str, artifact: str, task_id: str = "1"):
    """Read a Metaflow artifact directly from the local datastore.

    Bypasses the Metaflow metadata service entirely — reads the content-addressed
    gzip+pickle files that the step commands write to disk.
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

        # Must have preamble comment
        assert "DO NOT EDIT" in code
        assert "LinearFlow" in code

        # Must define all three ops
        assert "def op_start(" in code
        assert "def op_process(" in code
        assert "def op_end(" in code

        # Must define the job
        assert "@job" in code
        assert "def LinearFlow(" in code

        # Must have Definitions export
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
        # Split op should fan out to both branches
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

        # Config class
        assert "class ParametrizedFlowConfig(Config):" in code
        assert "greeting" in code
        assert "count" in code
        # Config used in start op
        assert "ParametrizedFlowConfig" in code

    def test_custom_job_name(self, tmp_path):
        out = tmp_path / "named_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        # --name is an option of the `create` subcommand, placed after the output file arg
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
            # ast.parse will raise SyntaxError if invalid
            import ast
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
# End-to-end execution tests
# ──────────────────────────────────────────────────────────────────────────────

class TestLinearFlow:

    def test_executes_successfully(self, tmp_path):
        out = tmp_path / "linear_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "linear_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.LinearFlow)
        assert result.success

    def test_all_steps_ran(self, tmp_path):
        out = tmp_path / "linear_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "linear_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.LinearFlow)
        ran = {e.node_name for e in result.all_node_events if hasattr(e, "node_name")}
        assert "op_start" in ran
        assert "op_process" in ran
        assert "op_end" in ran

    def test_metaflow_artifacts_persisted(self, tmp_path):
        out = tmp_path / "linear_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "linear_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.LinearFlow)

        # op_start returns "run_id/start/1" — extract run_id from it
        start_output = result.output_for_node("op_start")
        run_id = start_output.split("/")[0]

        value = _read_artifact(ds, "LinearFlow", run_id, "process", "result")
        assert value == "hello from start -> process"


class TestBranchingFlow:

    def test_executes_successfully(self, tmp_path):
        out = tmp_path / "branching_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "branching_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.BranchingFlow)
        assert result.success

    def test_both_branches_ran(self, tmp_path):
        out = tmp_path / "branching_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "branching_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.BranchingFlow)
        ran = {e.node_name for e in result.all_node_events if hasattr(e, "node_name")}
        assert "op_branch_a" in ran
        assert "op_branch_b" in ran
        assert "op_join" in ran

    def test_join_artifacts_correct(self, tmp_path):
        out = tmp_path / "branching_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "branching_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.BranchingFlow)
        # op_start for branching has multiple named outputs — use output_name
        start_output = result.output_for_node("op_start", output_name="branch_a")
        run_id = start_output.split("/")[0]

        assert _read_artifact(ds, "BranchingFlow", run_id, "join", "merged_a") == 20
        assert _read_artifact(ds, "BranchingFlow", run_id, "join", "merged_b") == 15


class TestForeachFlow:

    def test_executes_successfully(self, tmp_path):
        out = tmp_path / "foreach_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "foreach_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.ForeachFlow)
        assert result.success

    def test_all_items_processed(self, tmp_path):
        out = tmp_path / "foreach_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "foreach_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.ForeachFlow)

        # Three DynamicOutputs → three op_process_item invocations
        process_events = [
            e for e in result.all_node_events
            if hasattr(e, "node_name") and e.node_name
            and e.node_name.startswith("op_process_item")
        ]
        step_successes = [e for e in process_events if "SUCCESS" in str(e.event_type)]
        assert len(step_successes) >= 3, (
            f"Expected 3 process_item successes, got {len(step_successes)}"
        )

    def test_foreach_artifacts_correct(self, tmp_path):
        out = tmp_path / "foreach_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "foreach_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.ForeachFlow)
        # op_start for foreach yields DynamicOutputs — use op_join's output instead
        join_output = result.output_for_node("op_join")
        run_id = join_output.split("/")[0]

        results = _read_artifact(ds, "ForeachFlow", run_id, "join", "results")
        assert sorted(results) == ["APPLE", "BANANA", "CHERRY"]


class TestParametrizedFlow:

    def test_default_parameters(self, tmp_path):
        out = tmp_path / "param_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "parametrized_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.ParametrizedFlow)
        assert result.success

    def test_custom_parameters(self, tmp_path):
        out = tmp_path / "param_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "parametrized_flow.py", out, ds)
        mod = _load_module(out)

        run_config = {
            "ops": {
                "op_start": {
                    "config": {
                        "greeting": "Hi",
                        "count": 5,
                    }
                }
            }
        }
        result = _run_job(mod.ParametrizedFlow, run_config=run_config)
        assert result.success

        start_output = result.output_for_node("op_start")
        run_id = start_output.split("/")[0]

        messages = _read_artifact(ds, "ParametrizedFlow", run_id, "start", "messages")
        assert len(messages) == 5
        assert all(m.startswith("Hi #") for m in messages)

    def test_parameter_config_schema(self, tmp_path):
        out = tmp_path / "param_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "parametrized_flow.py", out, ds)
        mod = _load_module(out)

        # Verify Config class has the right fields
        config_cls = mod.ParametrizedFlowConfig
        fields = config_cls.__fields__ if hasattr(config_cls, "__fields__") else config_cls.model_fields
        assert "greeting" in fields
        assert "count" in fields


# ──────────────────────────────────────────────────────────────────────────────
# Decorator feature tests (@retry, @timeout, @environment, @project, --workflow-timeout)
# ──────────────────────────────────────────────────────────────────────────────

class TestDecoratorFeatures:

    def test_retry_generates_retry_policy(self, tmp_path):
        """@retry(times=3) produces RetryPolicy(max_retries=3) on the op."""
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()

        assert "RetryPolicy" in code
        assert "max_retries=3" in code

    def test_retry_delay_generates_delay(self, tmp_path):
        """@retry(minutes_between_retries=2) produces delay=120 in RetryPolicy."""
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()

        assert "delay=120" in code

    def test_timeout_generates_op_tag(self, tmp_path):
        """@timeout(seconds=120) produces dagster/op_execution_timeout tag."""
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()

        assert "dagster/op_execution_timeout" in code
        assert '"120"' in code

    def test_environment_vars_in_extra_env(self, tmp_path):
        """@environment(vars={...}) embeds vars as extra_env in the op."""
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()

        assert "MY_VAR" in code
        assert "hello" in code
        assert "OTHER" in code

    def test_retry_count_uses_context_retry_number(self, tmp_path):
        """retry_count=context.retry_number is passed to every _run_step call."""
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        code = out.read_text()

        assert "retry_count=context.retry_number" in code

    def test_workflow_timeout_sets_job_tag(self, tmp_path):
        """--workflow-timeout adds dagster/max_runtime tag to the @job."""
        out = tmp_path / "timeout_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "linear_flow.py", out, ds, extra_args=["--workflow-timeout", "3600"])
        code = out.read_text()

        assert "dagster/max_runtime" in code
        assert "3600" in code

    def test_no_retry_no_retry_policy_on_ops(self, tmp_path):
        """Steps without @retry do not emit retry_policy= on their ops."""
        out = tmp_path / "linear_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "linear_flow.py", out, ds)
        code = out.read_text()

        assert "retry_policy=" not in code

    def test_retry_flow_executes(self, tmp_path):
        """A flow with @retry/@timeout/@environment decorators compiles and runs."""
        out = tmp_path / "retry_dagster.py"
        ds = tmp_path / "ds"
        ds.mkdir()
        _compile(FLOWS_DIR / "retry_flow.py", out, ds)
        mod = _load_module(out)

        result = _run_job(mod.RetryFlow)
        assert result.success

        start_output = result.output_for_node("op_start")
        run_id = start_output.split("/")[0]
        result_val = _read_artifact(ds, "RetryFlow", run_id, "process", "result")
        assert result_val == 2
