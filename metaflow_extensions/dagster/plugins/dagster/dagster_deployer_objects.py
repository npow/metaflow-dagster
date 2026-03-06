"""DeployedFlow and TriggeredRun objects for the Dagster Deployer plugin."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar, Optional

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    import metaflow
    import metaflow.runner.deployer_impl


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
