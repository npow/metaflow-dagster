import os
import sys

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.metaflow_config_funcs import config_values
from metaflow.util import get_username

from .dagster_compiler import DagsterCompiler


class DagsterException(MetaflowException):
    headline = "Dagster error"


VALID_NAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def _resolve_job_name(name, flow_name):
    if name:
        if not all(c in VALID_NAME_CHARS for c in name):
            raise MetaflowException(
                "Job name %r contains invalid characters. "
                "Use only letters, digits and underscores." % name
            )
        return name
    # Default: snake_case of flow name
    return flow_name


def _validate_workflow(flow, graph):
    # Validate parameters have defaults
    for var, param in flow._get_parameters():
        if "default" not in param.kwargs:
            raise MetaflowException(
                "Parameter *%s* does not have a default value. "
                "A default value is required when deploying to Dagster." % param.name
            )
    # Validate no parallel decorators
    for node in graph:
        if node.parallel_foreach:
            raise DagsterException(
                "Deploying flows with @parallel decorator to Dagster is not supported."
            )


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Dagster deployment.")
@click.pass_obj
def dagster(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)


@dagster.command(help="Compile this flow to a Dagster definitions file.")
@click.argument("file", required=True)
@click.option(
    "--name",
    default=None,
    type=str,
    help="Dagster job name. Defaults to the flow name.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Tag all objects produced by Dagster job runs with this tag. "
    "Can be specified multiple times.",
)
@click.option(
    "--with",
    "with_decorators",
    multiple=True,
    default=None,
    help="Inject a Metaflow step decorator at deploy time (repeatable), "
    "e.g. --with=sandbox or --with='resources:cpu=4'.",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    help="Metaflow namespace for the production run.",
)
@click.option(
    "--max-workers",
    default=16,
    show_default=True,
    help="Maximum number of concurrent Dagster workers.",
)
@click.pass_obj
def create(obj, file, name=None, tags=None, user_namespace=None, max_workers=16, with_decorators=None):
    if os.path.abspath(sys.argv[0]) == os.path.abspath(file):
        raise MetaflowException(
            "Dagster output file cannot be the same as the flow file."
        )

    job_name = _resolve_job_name(name, obj.flow.name)

    _validate_workflow(obj.flow, obj.graph)

    obj.echo(
        "Compiling *%s* to Dagster job *%s*..." % (obj.flow.name, job_name),
        bold=True,
    )

    flow_file = os.path.abspath(sys.argv[0])

    # Gather metaflow runtime configuration to embed in the generated file
    step_env = {
        k: v
        for k, v in config_values()
        if v is not None
        and k.startswith(
            (
                "METAFLOW_DEFAULT_",
                "METAFLOW_DATASTORE_",
                "METAFLOW_DATATOOLS_",
                "METAFLOW_SERVICE_",
                "METAFLOW_METADATA",
                "METAFLOW_DEBUG_",
            )
        )
    }

    compiler = DagsterCompiler(
        job_name=job_name,
        graph=obj.graph,
        flow=obj.flow,
        flow_file=flow_file,
        metadata=obj.metadata,
        environment=obj.environment,
        flow_datastore=obj.flow_datastore,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
        tags=list(tags) if tags else [],
        with_decorators=list(with_decorators) if with_decorators else [],
        namespace=user_namespace,
        username=get_username(),
        max_workers=max_workers,
        step_env=step_env,
    )

    with open(file, "w") as f:
        f.write(compiler.compile())

    obj.echo(
        "Dagster job *{job_name}* for flow *{flow_name}* written to *{file}*.\n"
        "Load it in Dagster with:\n"
        "    dagster dev -f {file}".format(
            job_name=job_name,
            flow_name=obj.flow.name,
            file=file,
        ),
        bold=True,
    )
