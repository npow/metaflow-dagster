"""Unit tests for utility functions in dagster_cli.py."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from metaflow_extensions.dagster.plugins.dagster.dagster_cli import (
    DagsterException,
    NotSupportedException,
    _fix_local_step_metadata,
    _gather_step_env,
    _resolve_job_name,
    _validate_workflow,
)
from metaflow.exception import MetaflowException


# ──────────────────────────────────────────────────────────────────────────────
# _fix_local_step_metadata
# ──────────────────────────────────────────────────────────────────────────────


class TestFixLocalStepMetadata:
    def test_does_nothing_when_run_dir_does_not_exist(self, tmp_path):
        # Should not raise even if run dir is missing
        _fix_local_step_metadata.__wrapped__ = None  # reset if decorated
        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.DATASTORE_LOCAL_DIR",
            None,
            create=True,
        ):
            with patch.dict(os.environ, {"HOME": str(tmp_path)}):
                # No run directory exists — should silently succeed
                _fix_local_step_metadata("TestFlow", "run123")

    def test_creates_step_self_json(self, tmp_path):
        flow_name = "TestFlow"
        run_id = "dagster-abc123"
        ds_dir = tmp_path / ".metaflow"
        run_dir = ds_dir / flow_name / run_id
        step_dir = run_dir / "start"
        step_dir.mkdir(parents=True)
        # No _self.json yet

        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.DATASTORE_LOCAL_DIR",
            str(ds_dir.relative_to(tmp_path)),
            create=True,
        ):
            with patch.dict(os.environ, {"HOME": str(tmp_path)}):
                _fix_local_step_metadata(flow_name, run_id)

        step_self = step_dir / "_meta" / "_self.json"
        assert step_self.exists()
        data = json.loads(step_self.read_text())
        assert data["step_name"] == "start"
        assert data["flow_id"] == flow_name

    def test_creates_task_self_json(self, tmp_path):
        flow_name = "TestFlow"
        run_id = "dagster-abc123"
        ds_dir = tmp_path / ".metaflow"
        run_dir = ds_dir / flow_name / run_id
        task_dir = run_dir / "start" / "1"
        task_dir.mkdir(parents=True)

        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.DATASTORE_LOCAL_DIR",
            str(ds_dir.relative_to(tmp_path)),
            create=True,
        ):
            with patch.dict(os.environ, {"HOME": str(tmp_path)}):
                _fix_local_step_metadata(flow_name, run_id)

        task_self = task_dir / "_meta" / "_self.json"
        assert task_self.exists()
        data = json.loads(task_self.read_text())
        assert data["task_id"] == "1"
        assert data["step_name"] == "start"

    def test_does_not_overwrite_existing_step_self_json(self, tmp_path):
        flow_name = "TestFlow"
        run_id = "dagster-abc123"
        ds_dir = tmp_path / ".metaflow"
        run_dir = ds_dir / flow_name / run_id
        step_dir = run_dir / "start"
        step_meta_dir = step_dir / "_meta"
        step_meta_dir.mkdir(parents=True)
        existing = {"custom": "data"}
        (step_meta_dir / "_self.json").write_text(json.dumps(existing))

        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.DATASTORE_LOCAL_DIR",
            str(ds_dir.relative_to(tmp_path)),
            create=True,
        ):
            with patch.dict(os.environ, {"HOME": str(tmp_path)}):
                _fix_local_step_metadata(flow_name, run_id)

        data = json.loads((step_meta_dir / "_self.json").read_text())
        assert data == existing  # not overwritten

    def test_skips_meta_directories(self, tmp_path):
        flow_name = "TestFlow"
        run_id = "dagster-abc123"
        ds_dir = tmp_path / ".metaflow"
        run_dir = ds_dir / flow_name / run_id
        # Create a _meta directory at step level — should be skipped
        (run_dir / "_meta" / "something").mkdir(parents=True)

        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.DATASTORE_LOCAL_DIR",
            str(ds_dir.relative_to(tmp_path)),
            create=True,
        ):
            with patch.dict(os.environ, {"HOME": str(tmp_path)}):
                # Should not raise or create _self.json inside _meta dir
                _fix_local_step_metadata(flow_name, run_id)

    def test_silently_handles_exception(self, tmp_path):
        # Simulate an exception inside the try block — should not propagate.
        with patch("os.path.expanduser", side_effect=RuntimeError("boom")):
            # Should not raise
            _fix_local_step_metadata("TestFlow", "run123")


# ──────────────────────────────────────────────────────────────────────────────
# _resolve_job_name
# ──────────────────────────────────────────────────────────────────────────────


class TestResolveJobName:
    def test_returns_name_when_given(self):
        assert _resolve_job_name("my_job", "MyFlow") == "my_job"

    def test_raises_on_invalid_chars(self):
        with pytest.raises(MetaflowException, match="invalid characters"):
            _resolve_job_name("my-job", "MyFlow")

    def test_raises_on_space(self):
        with pytest.raises(MetaflowException, match="invalid characters"):
            _resolve_job_name("my job", "MyFlow")

    def test_falls_back_to_flow_name(self):
        assert _resolve_job_name(None, "MyFlow") == "MyFlow"

    def test_uses_project_name_when_available(self):
        flow = MagicMock()
        deco = MagicMock()
        deco.attributes = {"name": "my_project"}
        flow._flow_decorators.get.return_value = [deco]
        result = _resolve_job_name(None, "MyFlow", flow)
        assert result == "my_project_MyFlow"

    def test_falls_back_when_no_project_deco(self):
        flow = MagicMock()
        flow._flow_decorators.get.return_value = None
        result = _resolve_job_name(None, "MyFlow", flow)
        assert result == "MyFlow"

    def test_falls_back_when_project_name_not_set(self):
        flow = MagicMock()
        deco = MagicMock()
        deco.attributes = {}
        flow._flow_decorators.get.return_value = [deco]
        result = _resolve_job_name(None, "MyFlow", flow)
        assert result == "MyFlow"

    def test_handles_exception_in_flow_decorators(self):
        flow = MagicMock()
        flow._flow_decorators.get.side_effect = AttributeError("no attrs")
        # Should silently fall back to flow_name
        result = _resolve_job_name(None, "MyFlow", flow)
        assert result == "MyFlow"


# ──────────────────────────────────────────────────────────────────────────────
# _validate_workflow
# ──────────────────────────────────────────────────────────────────────────────


def _make_graph_node(name="start", parallel_foreach=False, decorators=None):
    node = MagicMock()
    node.name = name
    node.parallel_foreach = parallel_foreach
    node.decorators = decorators or []
    return node


def _make_param(name="p", has_default=True):
    param = MagicMock()
    param.name = name
    param.kwargs = {"default": "val"} if has_default else {}
    return param


class TestValidateWorkflow:
    def test_passes_valid_flow(self):
        flow = MagicMock()
        flow._get_parameters.return_value = [("p", _make_param())]
        flow._flow_decorators.get.return_value = None
        graph = [_make_graph_node()]
        # Should not raise
        _validate_workflow(flow, graph)

    def test_raises_when_param_has_no_default(self):
        flow = MagicMock()
        flow._get_parameters.return_value = [("p", _make_param(has_default=False))]
        flow._flow_decorators.get.return_value = None
        graph = []
        with pytest.raises(MetaflowException, match="does not have a default value"):
            _validate_workflow(flow, graph)

    def test_raises_when_parallel_foreach(self):
        flow = MagicMock()
        flow._get_parameters.return_value = []
        flow._flow_decorators.get.return_value = None
        graph = [_make_graph_node(parallel_foreach=True)]
        with pytest.raises(DagsterException, match="@parallel"):
            _validate_workflow(flow, graph)

    def test_raises_when_slurm_decorator(self):
        flow = MagicMock()
        flow._get_parameters.return_value = []
        flow._flow_decorators.get.return_value = None
        slurm_deco = MagicMock()
        slurm_deco.name = "slurm"
        graph = [_make_graph_node(decorators=[slurm_deco])]
        with pytest.raises(DagsterException, match="@slurm"):
            _validate_workflow(flow, graph)

    def test_raises_when_exit_hook(self):
        flow = MagicMock()
        flow._get_parameters.return_value = []
        flow._flow_decorators.get.side_effect = lambda name: ["hook"] if name == "exit_hook" else None
        graph = []
        with pytest.raises(DagsterException, match="@exit_hook"):
            _validate_workflow(flow, graph)

    def test_passes_when_unsupported_deco_is_not_slurm(self):
        flow = MagicMock()
        flow._get_parameters.return_value = []
        flow._flow_decorators.get.return_value = None
        deco = MagicMock()
        deco.name = "resources"
        graph = [_make_graph_node(decorators=[deco])]
        # Should not raise
        _validate_workflow(flow, graph)


# ──────────────────────────────────────────────────────────────────────────────
# _gather_step_env
# ──────────────────────────────────────────────────────────────────────────────


class TestGatherStepEnv:
    def test_includes_metaflow_default_keys(self):
        mock_config = [
            ("METAFLOW_DEFAULT_METADATA", "local"),
            ("METAFLOW_DEFAULT_DATASTORE", "s3"),
            ("OTHER_KEY", "ignored"),
        ]
        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.config_values",
            return_value=mock_config,
        ):
            result = _gather_step_env()
        assert result["METAFLOW_DEFAULT_METADATA"] == "local"
        assert result["METAFLOW_DEFAULT_DATASTORE"] == "s3"
        assert "OTHER_KEY" not in result

    def test_excludes_none_values(self):
        mock_config = [
            ("METAFLOW_DEFAULT_METADATA", None),
            ("METAFLOW_DEFAULT_DATASTORE", "local"),
        ]
        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.config_values",
            return_value=mock_config,
        ):
            result = _gather_step_env()
        assert "METAFLOW_DEFAULT_METADATA" not in result
        assert result["METAFLOW_DEFAULT_DATASTORE"] == "local"

    def test_includes_service_keys(self):
        mock_config = [
            ("METAFLOW_SERVICE_URL", "http://localhost:8080"),
        ]
        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.config_values",
            return_value=mock_config,
        ):
            result = _gather_step_env()
        assert result["METAFLOW_SERVICE_URL"] == "http://localhost:8080"

    def test_includes_datastore_keys(self):
        mock_config = [
            ("METAFLOW_DATASTORE_SYSROOT_LOCAL", "/home/runner"),
        ]
        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_cli.config_values",
            return_value=mock_config,
        ):
            result = _gather_step_env()
        assert result["METAFLOW_DATASTORE_SYSROOT_LOCAL"] == "/home/runner"
