"""DeployedFlow and TriggeredRun objects for the Dagster Deployer plugin."""

from __future__ import annotations

import contextlib
import json
import os
import sys
from typing import TYPE_CHECKING, ClassVar, Optional

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    import metaflow
    import metaflow.runner.deployer_impl


@contextlib.contextmanager
def _patched_env(**vars: str):
    """Temporarily override environment variables, restoring originals on exit."""
    old = {k: os.environ.get(k) for k in vars}
    try:
        for k, v in vars.items():
            os.environ[k] = v
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class DagsterTriggeredRun(TriggeredRun):
    """A Dagster job run that was triggered via the Deployer API.

    Inherits ``.run`` from :class:`~metaflow.runner.deployer.TriggeredRun`, which polls
    Metaflow until the run with ``pathspec`` (``FlowName/dagster-<run_id>``) appears.
    """

    @property
    def dagster_ui(self) -> Optional[str]:
        """URL to the Dagster UI for this run, if available.

        Returns a link to the local Dagster UI (http://localhost:3000) by default.
        The run ID embedded in the Metaflow pathspec is the Dagster run UUID.
        """
        try:
            _, run_id = self.pathspec.split("/")
            if run_id.startswith("dagster-"):
                return "http://localhost:3000/runs/%s" % run_id[len("dagster-"):]
        except Exception:
            pass
        return None

    @property
    def run(self):
        """Retrieve the Run object, applying deployer env vars so local metadata works."""
        import metaflow
        from metaflow.exception import MetaflowNotFound

        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        if meta_type == "local" and sysroot is None:
            sysroot = os.path.expanduser("~")

        patch = {}
        if meta_type:
            patch["METAFLOW_DEFAULT_METADATA"] = meta_type
        if sysroot:
            patch["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot

        try:
            with _patched_env(**patch):
                if meta_type:
                    metaflow.metadata(meta_type)
                return metaflow.Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None

    @property
    def status(self) -> Optional[str]:
        """Return a simple status string based on the underlying Metaflow run."""
        run = self.run
        if run is None:
            return "PENDING"
        if run.successful:
            return "SUCCEEDED"
        if run.finished:
            return "FAILED"
        return "RUNNING"


class DagsterDeployedFlow(DeployedFlow):
    """A Metaflow flow compiled as a Dagster definitions file."""

    TYPE: ClassVar[Optional[str]] = "dagster"

    @property
    def id(self) -> str:
        """Deployment identifier encoding all info needed for ``from_deployment``."""
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        return json.dumps({
            "name": self.name,
            "flow_name": self.flow_name,
            "flow_file": getattr(self.deployer, "flow_file", None),
            "definitions_file": additional_info.get("definitions_file"),
        })

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None) -> "DagsterDeployedFlow":
        """Recover a DagsterDeployedFlow from a deployment identifier.

        The identifier is the JSON string returned by ``deployed_flow.id``.
        """
        from .dagster_deployer import DagsterDeployer

        try:
            info = json.loads(identifier)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(
                "Invalid Dagster deployment identifier (expected JSON): %r" % identifier
            ) from exc
        for required in ("flow_file", "name", "flow_name"):
            if required not in info:
                raise ValueError(
                    "Deployment identifier missing required field %r" % required
                )
        flow_file = info["flow_file"]
        deployer = DagsterDeployer(flow_file=flow_file, deployer_kwargs={})
        deployer.name = info["name"]
        deployer.flow_name = info["flow_name"]
        deployer.metadata = metadata or "{}"
        deployer.additional_info = {
            "definitions_file": info.get("definitions_file"),
        }
        return cls(deployer=deployer)

    def run(self, **kwargs) -> DagsterTriggeredRun:
        """Trigger a new run of this deployed flow.

        Parameters
        ----------
        **kwargs : Any
            Flow parameters as keyword arguments (e.g. ``message="hello"``).

        Returns
        -------
        DagsterTriggeredRun
        """
        # Convert kwargs to "key=value" strings for --run-param.
        run_params = tuple("%s=%s" % (k, v) for k, v in kwargs.items())

        # Retrieve definitions_file from additional_info stored during create.
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        definitions_file = additional_info.get("definitions_file")

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = dict(deployer_attribute_file=attribute_file_path)
            if run_params:
                trigger_kwargs["run_params"] = run_params
            if definitions_file:
                trigger_kwargs["definitions_file"] = definitions_file
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(**trigger_kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )
            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return DagsterTriggeredRun(deployer=self.deployer, content=content)

        raise RuntimeError(
            "Error triggering Dagster job for flow %r" % self.deployer.flow_file
        )

    # Keep trigger() as an alias for backwards compatibility.
    trigger = run
