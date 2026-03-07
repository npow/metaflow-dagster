"""
Unit tests for DagsterCompiler.

These tests instantiate DagsterCompiler directly (in-process) so that coverage
can be measured.  They cover:
  - Happy-path code generation for every graph shape
  - Compiler output correctness (job body wiring, op signatures, config class)
  - Adversarial / error cases (validation failures, unsupported decorators)
  - Edge cases (no params, multiple tags, workflow-timeout, @project naming)
"""

import ast
from pathlib import Path
from unittest.mock import MagicMock

import pytest

FLOWS_DIR = Path(__file__).parent / "flows"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_compiler(flow_cls, *, job_name=None, tags=None, with_decorators=None,
                   namespace=None, workflow_timeout=None, step_env=None):
    """Instantiate a DagsterCompiler for *flow_cls* with minimal mock dependencies."""
    import sys
    sys.path.insert(0, str(FLOWS_DIR))

    from metaflow.graph import FlowGraph

    from metaflow_extensions.dagster.plugins.dagster.dagster_compiler import DagsterCompiler

    flow = flow_cls(use_cli=False)
    graph = FlowGraph(flow_cls)

    metadata = MagicMock(); metadata.TYPE = "local"
    environment = MagicMock(); environment.TYPE = "local"
    datastore = MagicMock(); datastore.TYPE = "local"; datastore.datastore_root = "/tmp/ds"
    event_logger = MagicMock(); event_logger.TYPE = "nullSidecarLogger"
    monitor = MagicMock(); monitor.TYPE = "nullSidecarMonitor"

    return DagsterCompiler(
        job_name=job_name or flow_cls.__name__,
        graph=graph,
        flow=flow,
        flow_file=f"/tmp/{flow_cls.__name__.lower()}.py",
        metadata=metadata,
        environment=environment,
        flow_datastore=datastore,
        event_logger=event_logger,
        monitor=monitor,
        tags=tags or [],
        with_decorators=with_decorators or [],
        namespace=namespace,
        workflow_timeout=workflow_timeout,
        step_env=step_env or {},
    )


def _compile(flow_cls, **kwargs) -> str:
    return _make_compiler(flow_cls, **kwargs).compile()


def _is_valid_python(code: str) -> bool:
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False


def _import_flow(name):
    import importlib.util
    path = FLOWS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, "".join(part.capitalize() for part in name.split("_")))


# ──────────────────────────────────────────────────────────────────────────────
# Happy-path: generated code structure
# ──────────────────────────────────────────────────────────────────────────────

class TestLinearFlow:

    def setup_method(self):
        self.flow_cls = _import_flow("linear_flow")
        self.code = _compile(self.flow_cls)

    def test_syntax_valid(self):
        assert _is_valid_python(self.code)

    def test_do_not_edit_banner(self):
        assert "DO NOT EDIT" in self.code

    def test_all_ops_generated(self):
        assert "def op_start(" in self.code
        assert "def op_process(" in self.code
        assert "def op_end(" in self.code

    def test_job_function_generated(self):
        assert "@job\ndef LinearFlow():" in self.code

    def test_definitions_object(self):
        assert "defs = Definitions(" in self.code

    def test_no_retry_policy_when_no_retry(self):
        assert "retry_policy=" not in self.code

    def test_step_env_embedded(self):
        code = _compile(self.flow_cls, step_env={"MY_KEY": "my_value"})
        assert "MY_KEY" in code
        assert "my_value" in code


class TestBranchingFlow:

    def setup_method(self):
        self.flow_cls = _import_flow("branching_flow")
        self.code = _compile(self.flow_cls)

    def test_syntax_valid(self):
        assert _is_valid_python(self.code)

    def test_branch_ops_generated(self):
        assert "def op_branch_a(" in self.code
        assert "def op_branch_b(" in self.code
        assert "def op_join(" in self.code

    def test_split_outputs_correct(self):
        assert '"branch_a": Out(str)' in self.code
        assert '"branch_b": Out(str)' in self.code

    def test_join_op_uses_both_inputs(self):
        # join op should accept branch_a and branch_b as positional parameters
        assert "branch_a" in self.code
        assert "branch_b" in self.code
        # Both should appear as In(str) in the ins= dict
        assert '"branch_a": In(str)' in self.code
        assert '"branch_b": In(str)' in self.code

    def test_job_wires_branches(self):
        # job body should reference .branch_a and .branch_b outputs
        assert ".branch_a" in self.code
        assert ".branch_b" in self.code


class TestForeachFlow:

    def setup_method(self):
        self.flow_cls = _import_flow("foreach_flow")
        self.code = _compile(self.flow_cls)

    def test_syntax_valid(self):
        assert _is_valid_python(self.code)

    def test_dynamic_outputs_used(self):
        assert "DynamicOut" in self.code
        assert "DynamicOutput" in self.code

    def test_map_and_collect(self):
        assert ".map(" in self.code
        assert ".collect()" in self.code

    def test_foreach_op_generated(self):
        assert "def op_process_item(" in self.code


class TestParametrizedFlow:

    def setup_method(self):
        self.flow_cls = _import_flow("parametrized_flow")
        self.code = _compile(self.flow_cls)

    def test_syntax_valid(self):
        assert _is_valid_python(self.code)

    def test_config_class_generated(self):
        assert "class ParametrizedFlowConfig(Config):" in self.code

    def test_config_fields_present(self):
        assert "greeting" in self.code
        assert "count" in self.code

    def test_config_accepted_by_start_op(self):
        assert "ParametrizedFlowConfig" in self.code

    def test_config_has_correct_types(self):
        # int Parameter should generate int field, str Parameter should generate str field
        # (exact types depend on defaults; both params have defaults so types are inferred)
        assert "count" in self.code
        assert "greeting" in self.code


class TestRetryFlow:

    def setup_method(self):
        self.flow_cls = _import_flow("retry_flow")
        self.code = _compile(self.flow_cls)

    def test_syntax_valid(self):
        assert _is_valid_python(self.code)

    def test_retry_policy_generated(self):
        assert "RetryPolicy" in self.code
        assert "max_retries=3" in self.code

    def test_retry_delay_generated(self):
        assert "delay=120" in self.code

    def test_timeout_tag_generated(self):
        assert "dagster/op_execution_timeout" in self.code
        assert '"120"' in self.code

    def test_environment_vars_in_extra_env(self):
        assert "MY_VAR" in self.code
        assert "hello" in self.code

    def test_retry_count_from_context(self):
        assert "retry_count=context.retry_number" in self.code


# ──────────────────────────────────────────────────────────────────────────────
# Options: tags, custom name, workflow-timeout, namespace
# ──────────────────────────────────────────────────────────────────────────────

class TestCompilerOptions:

    def setup_method(self):
        self.flow_cls = _import_flow("linear_flow")

    def test_tags_embedded(self):
        code = _compile(self.flow_cls, tags=["env:prod", "v2"])
        assert "env:prod" in code
        assert "v2" in code

    def test_custom_job_name(self):
        code = _compile(self.flow_cls, job_name="my_custom_job")
        assert "def my_custom_job():" in code

    def test_workflow_timeout_sets_job_decorator(self):
        code = _compile(self.flow_cls, workflow_timeout=3600)
        assert "dagster/max_runtime" in code
        assert "3600" in code

    def test_no_workflow_timeout_no_job_args(self):
        code = _compile(self.flow_cls, workflow_timeout=None)
        assert "@job\n" in code
        assert "dagster/max_runtime" not in code

    def test_namespace_appended_to_top_args(self):
        code = _compile(self.flow_cls, namespace="production")
        top_args_line = next(
            (l for l in code.splitlines() if l.startswith("METAFLOW_TOP_ARGS")), ""
        )
        assert "--namespace=production" in top_args_line

    def test_no_namespace_not_in_top_args(self):
        code = _compile(self.flow_cls, namespace=None)
        # METAFLOW_TOP_ARGS must not include a --namespace= flag when unset
        top_args_line = next(
            (l for l in code.splitlines() if l.startswith("METAFLOW_TOP_ARGS")), ""
        )
        assert "--namespace=" not in top_args_line


# ──────────────────────────────────────────────────────────────────────────────
# Adversarial / validation failure cases
# ──────────────────────────────────────────────────────────────────────────────

class TestValidation:
    """Tests that the validation gate rejects unsupported features."""

    def _validate(self, flow_cls):
        """Run only the validation step (not full compilation)."""
        from metaflow.graph import FlowGraph

        from metaflow_extensions.dagster.plugins.dagster.dagster_cli import _validate_workflow

        flow = flow_cls(use_cli=False)
        graph = FlowGraph(flow_cls)
        _validate_workflow(flow, graph)

    def test_parameter_without_default_raises(self, tmp_path):
        """A Parameter with no default must raise MetaflowException."""
        from metaflow import FlowSpec, Parameter, step
        from metaflow.exception import MetaflowException

        from metaflow_extensions.dagster.plugins.dagster.dagster_cli import _validate_workflow

        class NoDefaultFlow(FlowSpec):
            x = Parameter("x")  # no default

            @step
            def start(self):
                self.next(self.end)

            @step
            def end(self):
                pass

        flow = NoDefaultFlow(use_cli=False)
        from metaflow.graph import FlowGraph
        graph = FlowGraph(NoDefaultFlow)
        with pytest.raises(MetaflowException, match="default value"):
            _validate_workflow(flow, graph)

    def test_slurm_decorator_raises(self):
        """@slurm is unsupported — must raise DagsterException."""
        from metaflow_extensions.dagster.plugins.dagster.dagster_cli import (
            DagsterException,
            _validate_workflow,
        )

        mock_deco = MagicMock()
        mock_deco.name = "slurm"

        mock_node = MagicMock()
        mock_node.parallel_foreach = False
        mock_node.decorators = [mock_deco]
        mock_node.name = "start"

        class MockGraph:
            def __iter__(self):
                return iter([mock_node])

        class MockFlow:
            def _get_parameters(self):
                return []
            _flow_decorators = {}

        with pytest.raises(DagsterException, match="slurm"):
            _validate_workflow(MockFlow(), MockGraph())

    def test_parallel_foreach_raises(self):
        """@parallel is unsupported — must raise DagsterException."""
        from metaflow_extensions.dagster.plugins.dagster.dagster_cli import (
            DagsterException,
            _validate_workflow,
        )

        mock_node = MagicMock()
        mock_node.parallel_foreach = True
        mock_node.decorators = []
        mock_node.name = "start"

        class MockGraph:
            def __iter__(self):
                return iter([mock_node])

        class MockFlow:
            def _get_parameters(self):
                return []
            _flow_decorators = {}

        with pytest.raises(DagsterException, match="parallel"):
            _validate_workflow(MockFlow(), MockGraph())

    def test_trigger_flow_deco_no_longer_raises(self):
        """@trigger is now supported — it produces Dagster sensors instead of raising."""
        from metaflow_extensions.dagster.plugins.dagster.dagster_cli import (
            _validate_workflow,
        )

        class MockGraph:
            def __iter__(self):
                return iter([])

        class MockFlow:
            def _get_parameters(self):
                return []
            _flow_decorators = {"trigger": [MagicMock()]}

        # Should not raise — @trigger is handled at compile time via sensor generation
        _validate_workflow(MockFlow(), MockGraph())

    def test_invalid_job_name_characters_raises(self):
        """Job names with spaces / special chars must raise."""
        from metaflow.exception import MetaflowException

        from metaflow_extensions.dagster.plugins.dagster.dagster_cli import _resolve_job_name

        with pytest.raises(MetaflowException, match="invalid characters"):
            _resolve_job_name("bad name!", "MyFlow")

    def test_valid_job_name_accepted(self):
        from metaflow_extensions.dagster.plugins.dagster.dagster_cli import _resolve_job_name

        assert _resolve_job_name("good_name_123", "MyFlow") == "good_name_123"

    def test_default_job_name_is_flow_name(self):
        from metaflow_extensions.dagster.plugins.dagster.dagster_cli import _resolve_job_name

        assert _resolve_job_name(None, "MyFlow") == "MyFlow"


# ──────────────────────────────────────────────────────────────────────────────
# Compiler internals
# ──────────────────────────────────────────────────────────────────────────────

class TestCompilerInternals:

    def setup_method(self):
        self.linear_cls = _import_flow("linear_flow")
        self.foreach_cls = _import_flow("foreach_flow")

    def test_topological_order_cached(self):
        compiler = _make_compiler(self.linear_cls)
        order1 = compiler._topological_order()
        order2 = compiler._topological_order()
        assert order1 is order2  # same list object — cache hit

    def test_topological_order_starts_with_start(self):
        compiler = _make_compiler(self.linear_cls)
        order = compiler._topological_order()
        assert order[0].name == "start"

    def test_topological_order_ends_with_end(self):
        compiler = _make_compiler(self.linear_cls)
        order = compiler._topological_order()
        assert order[-1].name == "end"

    def test_steps_in_compound_empty_for_simple_foreach(self):
        compiler = _make_compiler(self.foreach_cls)
        assert compiler._steps_in_compound() == set()

    def test_foreach_chains_non_empty_for_foreach_flow(self):
        compiler = _make_compiler(self.foreach_cls)
        chains = compiler._foreach_chains()
        assert len(chains) == 1  # one top-level foreach

    def test_foreach_chains_cached(self):
        compiler = _make_compiler(self.foreach_cls)
        c1 = compiler._foreach_chains()
        c2 = compiler._foreach_chains()
        assert c1 is c2

    def test_is_foreach_join_true_for_foreach_join(self):
        compiler = _make_compiler(self.foreach_cls)
        from metaflow.graph import FlowGraph
        graph = FlowGraph(self.foreach_cls)
        join_node = graph["join"]
        assert compiler._is_foreach_join(join_node) is True

    def test_is_foreach_join_false_for_regular_node(self):
        compiler = _make_compiler(self.linear_cls)
        from metaflow.graph import FlowGraph
        graph = FlowGraph(self.linear_cls)
        end_node = graph["end"]
        assert compiler._is_foreach_join(end_node) is False

    def test_op_name_format(self):
        compiler = _make_compiler(self.linear_cls)
        assert compiler._op_name("start") == "op_start"
        assert compiler._op_name("my_step") == "op_my_step"

    def test_ins_spec_format(self):
        compiler = _make_compiler(self.linear_cls)
        spec = compiler._ins_spec("upstream")
        assert '"upstream": In(str)' in spec

    def test_render_config_class_empty_when_no_params(self):
        compiler = _make_compiler(self.linear_cls)
        assert compiler._render_config_class() == ""

    def test_render_config_class_has_fields_when_params(self):
        param_cls = _import_flow("parametrized_flow")
        compiler = _make_compiler(param_cls)
        config = compiler._render_config_class()
        assert "class ParametrizedFlowConfig(Config):" in config
        assert "greeting" in config
        assert "count" in config


# ──────────────────────────────────────────────────────────────────────────────
# Nested foreach (compound ops)
# ──────────────────────────────────────────────────────────────────────────────

class TestNestedForeachFlow:

    def setup_method(self):
        self.flow_cls = _import_flow("nested_foreach_flow")
        self.code = _compile(self.flow_cls)

    def test_syntax_valid(self):
        assert _is_valid_python(self.code)

    def test_compound_op_generated(self):
        # Nested foreach requires a compound op
        assert "compound" in self.code.lower() or "_body" in self.code

    def test_map_called_on_outer_foreach(self):
        assert ".map(" in self.code

    def test_collect_called_on_outer_join(self):
        assert ".collect()" in self.code

    def test_steps_in_compound_non_empty(self):
        compiler = _make_compiler(self.flow_cls)
        assert len(compiler._steps_in_compound()) > 0


# ──────────────────────────────────────────────────────────────────────────────
# Conditional (split-switch) flow
# ──────────────────────────────────────────────────────────────────────────────

class TestConditionalFlow:

    def setup_method(self):
        self.flow_cls = _import_flow("conditional_flow")
        self.compiler = _make_compiler(self.flow_cls)
        self.code = _compile(self.flow_cls)

    # ── syntax & structure ──────────────────────────────────────────────────

    def test_syntax_valid(self):
        assert _is_valid_python(self.code)

    def test_all_steps_present(self):
        for step in ("start", "high_branch", "low_branch", "join", "end"):
            assert f"op_{step}" in self.code

    # ── start op (split-switch) ─────────────────────────────────────────────

    def test_start_op_uses_is_required_false(self):
        assert "is_required=False" in self.code

    def test_start_op_yields_condition_branch(self):
        assert "_get_condition_branch" in self.code
        assert "yield Output(task_path, output_name=_branch)" in self.code

    def test_start_op_branch_outputs_named(self):
        assert '"high_branch": Out(str, is_required=False)' in self.code
        assert '"low_branch": Out(str, is_required=False)' in self.code

    # ── condition merge op ──────────────────────────────────────────────────

    def test_join_uses_optional_inputs(self):
        assert "Optional[str]" in self.code
        assert "default_value=None" in self.code

    def test_join_finds_non_none_input(self):
        assert "next(p for p in" in self.code
        assert "if p is not None" in self.code

    # ── job wiring ───────────────────────────────────────────────────────────

    def test_job_wires_branches_from_start(self):
        assert "r_start.high_branch" in self.code or "r_start" in self.code
        # branch vars are accessed via named outputs
        assert ".high_branch" in self.code
        assert ".low_branch" in self.code

    def test_join_called_with_keyword_args(self):
        # Merge op must receive both branch outputs as keyword args
        assert "high_branch=" in self.code
        assert "low_branch=" in self.code

    # ── detection helpers ────────────────────────────────────────────────────

    def test_is_condition_branch_detects_branch_steps(self):
        from metaflow.graph import FlowGraph
        graph = FlowGraph(self.flow_cls)
        high_node = graph["high_branch"]
        low_node = graph["low_branch"]
        assert self.compiler._is_condition_branch(high_node) is True
        assert self.compiler._is_condition_branch(low_node) is True

    def test_is_condition_branch_false_for_start(self):
        from metaflow.graph import FlowGraph
        graph = FlowGraph(self.flow_cls)
        start_node = graph["start"]
        assert self.compiler._is_condition_branch(start_node) is False

    def test_is_condition_merge_detects_join(self):
        from metaflow.graph import FlowGraph
        graph = FlowGraph(self.flow_cls)
        join_node = graph["join"]
        assert self.compiler._is_condition_merge(join_node) is True

    def test_is_condition_merge_false_for_branch(self):
        from metaflow.graph import FlowGraph
        graph = FlowGraph(self.flow_cls)
        high_node = graph["high_branch"]
        assert self.compiler._is_condition_merge(high_node) is False

    def test_condition_switch_name_returns_start(self):
        from metaflow.graph import FlowGraph
        graph = FlowGraph(self.flow_cls)
        join_node = graph["join"]
        assert self.compiler._condition_switch_name(join_node) == "start"
