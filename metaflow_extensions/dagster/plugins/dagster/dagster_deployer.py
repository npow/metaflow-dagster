"""Metaflow Deployer plugin for Dagster.

Registers ``TYPE = "dagster"`` so that ``Deployer(flow_file).dagster(...)``
is available and the UX test suite can parametrise ``--scheduler-type=dagster``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    from metaflow_extensions.dagster.plugins.dagster.dagster_deployer_objects import (
        DagsterDeployedFlow,
    )


class DagsterDeployer(DeployerImpl):
    """Deployer implementation for Dagster.

    Parameters
    ----------
    name : str, optional
        Dagster job name.  Defaults to the flow name.
    max_workers : int, optional
        Maximum number of concurrent Dagster workers (default 16).
    workflow_timeout : int, optional
        Maximum wall-clock seconds for the entire job run.
    """

    TYPE: ClassVar[str | None] = "dagster"

    def __init__(self, deployer_kwargs: dict[str, str], **kwargs) -> None:
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> dict[str, str]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> type[DagsterDeployedFlow]:
        from .dagster_deployer_objects import DagsterDeployedFlow

        return DagsterDeployedFlow

    def create(self, **kwargs) -> DagsterDeployedFlow:
        """Compile this flow as a Dagster definitions file.

        Parameters
        ----------
        definitions_file : str
            Path to write the generated Dagster definitions file.
        name : str, optional
            Dagster job name.
        tags : list[str], optional
            Tags to attach to job runs.
        max_workers : int, optional
            Maximum concurrent Dagster workers.
        workflow_timeout : int, optional
            Maximum wall-clock seconds for the entire job run.
        deployer_attribute_file : str, optional
            Write deployment info JSON here (Metaflow Deployer API internal).

        Returns
        -------
        DagsterDeployedFlow
        """
        from .dagster_deployer_objects import DagsterDeployedFlow

        return self._create(DagsterDeployedFlow, **kwargs)
