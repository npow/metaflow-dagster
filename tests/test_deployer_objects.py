"""Unit tests for dagster_deployer_objects.py."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from metaflow_extensions.dagster.plugins.dagster.dagster_deployer_objects import (
    DagsterDeployedFlow,
    DagsterTriggeredRun,
    _patched_env,
)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _make_identifier(
    name="my_job",
    flow_name="MyFlow",
    flow_file="/path/to/flow.py",
    definitions_file="/path/to/defs.py",
):
    return json.dumps({
        "name": name,
        "flow_name": flow_name,
        "flow_file": flow_file,
        "definitions_file": definitions_file,
    })


def _make_deployer(
    name="my_job",
    flow_name="MyFlow",
    flow_file="/path/to/flow.py",
    definitions_file="/path/to/defs.py",
    env_vars=None,
):
    """Return a minimal mock deployer with the attributes DagsterDeployedFlow needs."""
    deployer = MagicMock()
    identifier = _make_identifier(name=name, flow_name=flow_name,
                                   flow_file=flow_file, definitions_file=definitions_file)
    deployer.name = identifier
    deployer.flow_name = flow_name
    deployer.metadata = "{}"
    deployer.additional_info = {"definitions_file": definitions_file}
    deployer.env_vars = env_vars or {}
    deployer.flow_file = flow_file
    return deployer


# ──────────────────────────────────────────────────────────────────────────────
# _patched_env tests
# ──────────────────────────────────────────────────────────────────────────────


class TestPatchedEnv:
    def test_sets_env_var_inside_context(self):
        os.environ.pop("_TEST_PATCH_VAR", None)
        with _patched_env(_TEST_PATCH_VAR="hello"):
            assert os.environ["_TEST_PATCH_VAR"] == "hello"

    def test_removes_var_after_context_when_not_originally_set(self):
        os.environ.pop("_TEST_PATCH_VAR", None)
        with _patched_env(_TEST_PATCH_VAR="hello"):
            pass
        assert "_TEST_PATCH_VAR" not in os.environ

    def test_restores_original_value(self):
        os.environ["_TEST_PATCH_VAR"] = "original"
        try:
            with _patched_env(_TEST_PATCH_VAR="overridden"):
                assert os.environ["_TEST_PATCH_VAR"] == "overridden"
            assert os.environ["_TEST_PATCH_VAR"] == "original"
        finally:
            os.environ.pop("_TEST_PATCH_VAR", None)

    def test_multiple_vars(self):
        os.environ.pop("_TEST_VAR1", None)
        os.environ.pop("_TEST_VAR2", None)
        with _patched_env(_TEST_VAR1="a", _TEST_VAR2="b"):
            assert os.environ["_TEST_VAR1"] == "a"
            assert os.environ["_TEST_VAR2"] == "b"
        assert "_TEST_VAR1" not in os.environ
        assert "_TEST_VAR2" not in os.environ


# ──────────────────────────────────────────────────────────────────────────────
# DagsterTriggeredRun tests
# ──────────────────────────────────────────────────────────────────────────────


class TestDagsterTriggeredRun:
    def _make_run(self, pathspec, env_vars=None):
        deployer = MagicMock()
        deployer.env_vars = env_vars or {}
        content = json.dumps({"pathspec": pathspec})
        return DagsterTriggeredRun(deployer=deployer, content=content)

    def test_dagster_ui_returns_url_for_valid_pathspec(self):
        run = self._make_run("MyFlow/dagster-abc123def456")
        assert run.dagster_ui == "http://localhost:3000/runs/abc123def456"

    def test_dagster_ui_returns_none_for_non_dagster_run_id(self):
        run = self._make_run("MyFlow/sfn-abc123")
        assert run.dagster_ui is None

    def test_dagster_ui_returns_none_for_invalid_pathspec(self):
        run = self._make_run("invalid")
        assert run.dagster_ui is None

    def test_dagster_ui_strips_dagster_prefix(self):
        run = self._make_run("MyFlow/dagster-xyz789")
        assert run.dagster_ui == "http://localhost:3000/runs/xyz789"

    def test_status_pending_when_run_is_none(self):
        run = self._make_run("MyFlow/dagster-abc")
        with patch.object(DagsterTriggeredRun, "run", new_callable=PropertyMock, return_value=None):
            assert run.status == "PENDING"

    def test_status_succeeded(self):
        run = self._make_run("MyFlow/dagster-abc")
        mock_run = MagicMock()
        mock_run.successful = True
        mock_run.finished = True
        with patch.object(DagsterTriggeredRun, "run", new_callable=PropertyMock, return_value=mock_run):
            assert run.status == "SUCCEEDED"

    def test_status_failed(self):
        run = self._make_run("MyFlow/dagster-abc")
        mock_run = MagicMock()
        mock_run.successful = False
        mock_run.finished = True
        with patch.object(DagsterTriggeredRun, "run", new_callable=PropertyMock, return_value=mock_run):
            assert run.status == "FAILED"

    def test_status_running(self):
        run = self._make_run("MyFlow/dagster-abc")
        mock_run = MagicMock()
        mock_run.successful = False
        mock_run.finished = False
        with patch.object(DagsterTriggeredRun, "run", new_callable=PropertyMock, return_value=mock_run):
            assert run.status == "RUNNING"

    def test_run_returns_none_on_not_found(self):
        deployer = MagicMock()
        deployer.env_vars = {"METAFLOW_DEFAULT_METADATA": "local"}
        content = json.dumps({"pathspec": "MyFlow/dagster-abc"})
        triggered = DagsterTriggeredRun(deployer=deployer, content=content)

        # metaflow is imported locally inside the run property; patch via sys.modules
        import sys
        import metaflow as real_mf
        from metaflow.exception import MetaflowNotFound
        mock_mf = MagicMock()
        mock_mf.Run.side_effect = MetaflowNotFound("not found")
        mock_mf.metadata = real_mf.metadata
        sys.modules["_test_metaflow_mock"] = mock_mf
        original = sys.modules.get("metaflow")
        sys.modules["metaflow"] = mock_mf
        try:
            result = triggered.run
            assert result is None
        finally:
            if original is not None:
                sys.modules["metaflow"] = original
            else:
                del sys.modules["metaflow"]


# ──────────────────────────────────────────────────────────────────────────────
# DagsterDeployedFlow tests
# ──────────────────────────────────────────────────────────────────────────────


class TestDagsterDeployedFlow:
    def test_name_extracted_from_json_identifier(self):
        deployer = _make_deployer(name="my_job")
        flow = DagsterDeployedFlow(deployer=deployer)
        assert flow.name == "my_job"

    def test_name_kept_as_is_when_not_json(self):
        deployer = MagicMock()
        deployer.name = "plain_name"
        deployer.flow_name = "MyFlow"
        deployer.metadata = "{}"
        deployer.additional_info = {}
        flow = DagsterDeployedFlow(deployer=deployer)
        assert flow.name == "plain_name"

    def test_id_returns_deployer_name(self):
        deployer = _make_deployer(name="my_job", flow_file="/f.py")
        flow = DagsterDeployedFlow(deployer=deployer)
        assert flow.id == deployer.name

    def test_id_contains_flow_file(self):
        deployer = _make_deployer(name="my_job", flow_file="/path/to/flow.py")
        flow = DagsterDeployedFlow(deployer=deployer)
        info = json.loads(flow.id)
        assert info["flow_file"] == "/path/to/flow.py"

    def test_id_contains_definitions_file(self):
        deployer = _make_deployer(definitions_file="/path/to/defs.py")
        flow = DagsterDeployedFlow(deployer=deployer)
        info = json.loads(flow.id)
        assert info["definitions_file"] == "/path/to/defs.py"

    def test_from_deployment_raises_on_invalid_json(self):
        with pytest.raises(ValueError, match="expected JSON"):
            DagsterDeployedFlow.from_deployment("not-json")

    def test_from_deployment_raises_on_missing_flow_file(self):
        identifier = json.dumps({"name": "my_job", "flow_name": "MyFlow"})
        with pytest.raises(ValueError, match="flow_file"):
            DagsterDeployedFlow.from_deployment(identifier)

    def test_from_deployment_raises_on_missing_name(self):
        identifier = json.dumps({"flow_file": "/f.py", "flow_name": "MyFlow"})
        with pytest.raises(ValueError, match="name"):
            DagsterDeployedFlow.from_deployment(identifier)

    def test_from_deployment_raises_on_missing_flow_name(self):
        identifier = json.dumps({"flow_file": "/f.py", "name": "my_job"})
        with pytest.raises(ValueError, match="flow_name"):
            DagsterDeployedFlow.from_deployment(identifier)

    def test_from_deployment_constructs_deployed_flow(self):
        identifier = _make_identifier(name="my_job", flow_name="MyFlow", flow_file="/f.py")
        # DagsterDeployer is imported locally inside from_deployment via
        # "from .dagster_deployer import DagsterDeployer"; patch where it lives.
        mock_deployer_instance = MagicMock()
        mock_deployer_instance.name = identifier
        mock_deployer_instance.flow_name = "MyFlow"
        mock_deployer_instance.metadata = "{}"
        mock_deployer_instance.additional_info = {"definitions_file": "/path/to/defs.py"}
        with patch(
            "metaflow_extensions.dagster.plugins.dagster.dagster_deployer.DagsterDeployer",
            return_value=mock_deployer_instance,
        ):
            # Also need to patch the import inside from_deployment's local scope
            import sys
            import metaflow_extensions.dagster.plugins.dagster.dagster_deployer as dd_mod
            original_class = dd_mod.DagsterDeployer
            try:
                dd_mod.DagsterDeployer = MagicMock(return_value=mock_deployer_instance)
                flow = DagsterDeployedFlow.from_deployment(identifier)
                assert flow.name == "my_job"
            finally:
                dd_mod.DagsterDeployer = original_class

    def test_trigger_is_alias_for_run(self):
        deployer = _make_deployer()
        flow = DagsterDeployedFlow(deployer=deployer)
        # trigger and run should be the same underlying function
        assert flow.trigger.__func__ is flow.run.__func__

    def test_run_raises_on_failure(self):
        deployer = _make_deployer()
        flow = DagsterDeployedFlow(deployer=deployer)
        # The deployer API mock will fail — run() should raise RuntimeError
        with pytest.raises(Exception):
            flow.run(foo="bar")
