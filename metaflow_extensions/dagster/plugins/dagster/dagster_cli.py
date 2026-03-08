import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.metaflow_config_funcs import config_values

from .dagster_compiler import DagsterCompiler


class NotSupportedException(MetaflowException):
    headline = "Dagster not supported"


class DagsterException(MetaflowException):
    headline = "Dagster error"


VALID_NAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def _fix_local_step_metadata(flow_name, run_id):
    """Create missing step and task-level _self.json files for local metadata.

    When Metaflow steps are run individually via CLI (as dagster does), the
    step-level and task-level metadata registration may be skipped. This
    function fills in the gaps so that the Metaflow client can enumerate
    steps/tasks and determine run completion.
    """
    try:
        from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
        home = os.path.expanduser("~")
        local_dir = DATASTORE_LOCAL_DIR or ".metaflow"
        run_dir = os.path.join(home, local_dir, flow_name, run_id)
        if not os.path.isdir(run_dir):
            return
        ts = int(time.time() * 1000)
        for step_name in os.listdir(run_dir):
            step_dir = os.path.join(run_dir, step_name)
            if step_name.startswith("_meta") or not os.path.isdir(step_dir):
                continue
            # Fix step-level metadata
            step_meta_dir = os.path.join(step_dir, "_meta")
            step_self = os.path.join(step_meta_dir, "_self.json")
            if not os.path.exists(step_self):
                os.makedirs(step_meta_dir, exist_ok=True)
                with open(step_self, "w") as f:
                    json.dump({
                        "flow_id": flow_name,
                        "run_number": run_id,
                        "step_name": step_name,
                        "ts_epoch": ts,
                    }, f)
            # Fix task-level metadata
            for task_id in os.listdir(step_dir):
                task_dir = os.path.join(step_dir, task_id)
                if task_id.startswith("_meta") or not os.path.isdir(task_dir):
                    continue
                task_meta_dir = os.path.join(task_dir, "_meta")
                task_self = os.path.join(task_meta_dir, "_self.json")
                if not os.path.exists(task_self):
                    os.makedirs(task_meta_dir, exist_ok=True)
                    with open(task_self, "w") as f:
                        json.dump({
                            "flow_id": flow_name,
                            "run_number": run_id,
                            "step_name": step_name,
                            "task_id": task_id,
                            "ts_epoch": ts,
                        }, f)
    except Exception:
        pass  # Best-effort; don't fail if local metadata fixup fails


def _resolve_job_name(name, flow_name, flow=None):
    if name:
        if not all(c in VALID_NAME_CHARS for c in name):
            raise MetaflowException(
                f"Job name {name!r} contains invalid characters. "
                "Use only letters, digits and underscores."
            )
        return name
    # Detect @project(name=...) and prefix: project_FlowName
    if flow is not None:
        try:
            project_decos = flow._flow_decorators.get("project")
        except Exception:
            project_decos = None
        if project_decos:
            project_name = project_decos[0].attributes.get("name")
            if project_name:
                return f"{project_name}_{flow_name}"
    return flow_name


def _validate_workflow(flow, graph):
    # Validate parameters have defaults
    for var, param in flow._get_parameters():
        if "default" not in param.kwargs:
            raise MetaflowException(
                f"Parameter *{param.name}* does not have a default value. "
                "A default value is required when deploying to Dagster."
            )
    # Validate no parallel decorators or unsupported step decorators
    for node in graph:
        if node.parallel_foreach:
            raise DagsterException(
                "Deploying flows with @parallel decorator to Dagster is not supported."
            )
        for deco in node.decorators:
            if deco.name == "slurm":
                raise DagsterException(
                    f"Step *{node.name}* uses @slurm which is not supported with Dagster."
                )
            # @resources hints are forwarded to compute backends via STEP_WITH_DECORATORS
    # @trigger and @trigger_on_finish are extracted and wired as Dagster sensors at compile time
    # Validate no unsupported flow-level decorators
    for bad_deco in ("exit_hook",):
        if flow._flow_decorators.get(bad_deco):
            raise DagsterException(
                f"@{bad_deco} is not supported with Dagster deployments."
            )


def _gather_step_env():
    """Gather Metaflow runtime config keys to embed in the generated file."""
    return {
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


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Dagster deployment.")
@click.pass_obj
def dagster(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)


@dagster.command(help="Compile this flow to a Dagster definitions file.")
@click.argument("file", required=False, default=None)
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
    "--workflow-timeout",
    default=None,
    type=int,
    help="Maximum wall-clock seconds for the entire job run.",
)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write deployment info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def create(obj, file, name=None, tags=None, user_namespace=None,
           with_decorators=None, workflow_timeout=None, deployer_attribute_file=None):
    if file is None:
        file = f"{obj.flow.name.lower()}_dagster.py"
    if os.path.abspath(sys.argv[0]) == os.path.abspath(file):
        raise MetaflowException(
            "Dagster output file cannot be the same as the flow file."
        )

    job_name = _resolve_job_name(name, obj.flow.name, obj.flow)

    _validate_workflow(obj.flow, obj.graph)

    obj.echo(
        f"Compiling *{obj.flow.name}* to Dagster job *{job_name}*...",
        bold=True,
    )

    flow_file = os.path.abspath(sys.argv[0])

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
        workflow_timeout=workflow_timeout,
        step_env=_gather_step_env(),
    )

    with open(file, "w") as f:
        f.write(compiler.compile())

    obj.echo(
        f"Dagster job *{job_name}* for flow *{obj.flow.name}* written to *{file}*.\n"
        "Load it in Dagster with:\n"
        f"    dagster dev -f {file}",
        bold=True,
    )

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": job_name,
                    "flow_name": obj.flow.name,
                    "metadata": "{}",
                    "additional_info": {"definitions_file": os.path.abspath(file)},
                },
                f,
            )


@dagster.command(help="Trigger a Dagster job execution.")
@click.option(
    "--definitions-file",
    default=None,
    help="Path to the generated Dagster definitions file. Defaults to <FlowName>_dagster.py.",
)
@click.option(
    "--job-name",
    default=None,
    type=str,
    help="Dagster job name. Defaults to the flow name.",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (repeatable).",
)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write triggered-run info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def trigger(obj, definitions_file, job_name=None, run_params=None, deployer_attribute_file=None):
    if definitions_file is None:
        definitions_file = f"{obj.flow.name.lower()}_dagster.py"
    resolved_job_name = _resolve_job_name(job_name, obj.flow.name, obj.flow)

    cmd = [
        sys.executable, "-m", "dagster", "job", "execute",
        "-f", definitions_file,
        "-j", resolved_job_name,
    ]

    config_yaml_lines = []
    for kv in (run_params or []):
        k, _, v = kv.partition("=")
        config_yaml_lines.append(f"{k.strip()}: {v.strip()}")

    config_file = None
    try:
        if config_yaml_lines:
            with tempfile.NamedTemporaryFile(
                suffix=".yaml", delete=False, mode="w"
            ) as tmp:
                tmp.write("ops:\n")
                # The compiler names ops as op_<step_name> (e.g. op_start).
                tmp.write("  op_start:\n")
                tmp.write("    config:\n")
                for line in config_yaml_lines:
                    tmp.write(f"      {line}\n")
                config_file = tmp.name
            cmd += ["-c", config_file]

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        # Echo output so it's visible when show_output is on
        if result.stdout:
            sys.stderr.write(result.stdout)
        if result.stderr:
            sys.stderr.write(result.stderr)

        if result.returncode != 0:
            raise DagsterException(
                f"dagster job execute returned exit code {result.returncode}."
            )
    finally:
        if config_file and os.path.exists(config_file):
            os.unlink(config_file)

    # Extract the Dagster run UUID from the job execute output so we can
    # derive the same deterministic Metaflow run-id used inside the job.
    combined_output = (result.stdout or "") + (result.stderr or "")
    match = re.search(r"\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b", combined_output)
    if match:
        run_id = "dagster-" + hashlib.sha1(match.group(1).encode()).hexdigest()[:12]
    else:
        run_id = f"dagster-{uuid.uuid4()}"

    # Fix local metadata: when steps are run individually via CLI, step-level
    # _self.json files may not be created. Create them so Metaflow's client
    # can enumerate steps and determine run completion.
    _fix_local_step_metadata(obj.flow.name, run_id)

    if deployer_attribute_file:
        pathspec = f"{obj.flow.name}/{run_id}"
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "pathspec": pathspec,
                    "job_name": resolved_job_name,
                    "metadata": {"flow_name": obj.flow.name},
                },
                f,
            )

    obj.echo(
        f"Dagster job *{resolved_job_name}* executed from *{definitions_file}*.",
        bold=True,
    )


@dagster.command(help="Resume a failed Dagster job run, reusing outputs from completed steps.")
@click.option(
    "--run-id",
    required=True,
    help="Metaflow run ID of the failed run to resume (e.g. dagster-abc123).",
)
@click.option(
    "--definitions-file",
    default=None,
    help="Path to the generated Dagster definitions file. Defaults to <FlowName>_dagster.py.",
)
@click.option(
    "--job-name",
    default=None,
    type=str,
    help="Dagster job name. Defaults to the flow name.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Tag for the new Metaflow run (repeatable).",
)
@click.option(
    "--with",
    "with_decorators",
    multiple=True,
    default=None,
    help="Inject a Metaflow step decorator at deploy time (repeatable).",
)
@click.option(
    "--workflow-timeout",
    default=None,
    type=int,
    help="Maximum wall-clock seconds for the entire job run.",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    help="Metaflow namespace for the resumed run.",
)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write resumed-run info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def resume(
    obj,
    run_id,
    definitions_file=None,
    job_name=None,
    tags=None,
    with_decorators=None,
    workflow_timeout=None,
    user_namespace=None,
    deployer_attribute_file=None,
):
    """Re-execute a failed Dagster job, skipping steps whose task output already exists.

    A new definitions file is compiled with ORIGIN_RUN_ID set to *run_id*.  When
    _run_step executes each Metaflow step it passes --clone-run-id to the Metaflow
    CLI so the runtime can reuse completed task outputs and skip re-running them.
    """
    if definitions_file is None:
        definitions_file = f"{obj.flow.name.lower()}_dagster.py"

    resolved_job_name = _resolve_job_name(job_name, obj.flow.name, obj.flow)

    _validate_workflow(obj.flow, obj.graph)

    flow_file = os.path.abspath(sys.argv[0])

    # Write a temporary definitions file with ORIGIN_RUN_ID embedded
    with tempfile.NamedTemporaryFile(
        suffix="_resume_dagster.py", delete=False, mode="w", dir=os.path.dirname(flow_file)
    ) as tmp:
        resume_defs_file = tmp.name

    try:
        compiler = DagsterCompiler(
            job_name=resolved_job_name,
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
            workflow_timeout=workflow_timeout,
            step_env=_gather_step_env(),
            origin_run_id=run_id,
        )

        with open(resume_defs_file, "w") as f:
            f.write(compiler.compile())

        obj.echo(
            f"Resuming *{obj.flow.name}* from run *{run_id}* as job *{resolved_job_name}*...",
            bold=True,
        )

        cmd = [
            sys.executable, "-m", "dagster", "job", "execute",
            "-f", resume_defs_file,
            "-j", resolved_job_name,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.stdout:
            sys.stderr.write(result.stdout)
        if result.stderr:
            sys.stderr.write(result.stderr)

        if result.returncode != 0:
            raise DagsterException(
                f"dagster job execute returned exit code {result.returncode}."
            )
    finally:
        try:
            os.unlink(resume_defs_file)
        except Exception:
            pass

    # Derive the new Metaflow run-id from the Dagster UUID in the output
    combined_output = (result.stdout or "") + (result.stderr or "")
    match = re.search(r"\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b", combined_output)
    if match:
        new_run_id = "dagster-" + hashlib.sha1(match.group(1).encode()).hexdigest()[:12]
    else:
        new_run_id = f"dagster-{uuid.uuid4()}"

    _fix_local_step_metadata(obj.flow.name, new_run_id)

    if deployer_attribute_file:
        pathspec = f"{obj.flow.name}/{new_run_id}"
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "pathspec": pathspec,
                    "job_name": resolved_job_name,
                    "origin_run_id": run_id,
                    "metadata": {"flow_name": obj.flow.name},
                },
                f,
            )

    obj.echo(
        f"Resumed Dagster job *{resolved_job_name}* (origin run: *{run_id}*, new run: *{new_run_id}*).",
        bold=True,
    )
