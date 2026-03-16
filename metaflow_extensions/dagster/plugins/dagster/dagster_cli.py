import hashlib
import json
import os
import sys
import tempfile
import time

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.metaflow_config_funcs import config_values

from .dagster_compiler import DagsterCompiler


class NotSupportedException(MetaflowException):
    headline = "Dagster not supported"


class DagsterException(MetaflowException):
    headline = "Dagster error"


VALID_NAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")

# Keys excluded from METAFLOW_STEP_ENV in the generated definitions file.
# These are sensitive credentials that must be provided at Dagster runtime
# via environment variables, not baked into a file that may be committed to VCS.
_STEP_ENV_DENYLIST = frozenset(
    {
        "METAFLOW_SERVICE_AUTH_KEY",
        "METAFLOW_SERVICE_INTERNAL_URL",
    }
)


def _fix_local_step_metadata(flow_name, run_id):
    """Create missing step and task-level _self.json files for local metadata.

    When Metaflow steps are run individually via CLI (as dagster does), the
    step-level and task-level metadata registration may be skipped. This
    function fills in the gaps so that the Metaflow client can enumerate
    steps/tasks and determine run completion.
    """
    try:
        from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
        local_dir = DATASTORE_LOCAL_DIR or ".metaflow"
        # Build candidate run directories to search:
        # 1. METAFLOW_DATASTORE_SYSROOT_LOCAL/<flow>/<run>  (direct sysroot, e.g. when sysroot
        #    is already under .metaflow, or the metadata provider uses it directly)
        # 2. METAFLOW_DATASTORE_SYSROOT_LOCAL/.metaflow/<flow>/<run>  (sysroot without .metaflow)
        # 3. ~/<DATASTORE_LOCAL_DIR>/<flow>/<run>  (legacy default path)
        candidates = []
        env_sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        if env_sysroot:
            candidates.append(os.path.join(env_sysroot, flow_name, run_id))
            candidates.append(os.path.join(env_sysroot, local_dir, flow_name, run_id))
        home = os.path.expanduser("~")
        candidates.append(os.path.join(home, local_dir, flow_name, run_id))
        run_dir = next((c for c in candidates if os.path.isdir(c)), None)
        if run_dir is None:
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
    for _var, param in flow._get_parameters():
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
    """Gather Metaflow runtime config keys to embed in the generated file.

    Keys in _STEP_ENV_DENYLIST are excluded: they are sensitive credentials
    that must be provided at Dagster runtime via environment variables rather
    than baked into a generated file that may be committed to VCS.
    """
    return {
        k: v
        for k, v in config_values()
        if v is not None
        and k not in _STEP_ENV_DENYLIST
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
def cli():  # pragma: no cover
    pass


@cli.group(help="Commands related to Dagster deployment.")
@click.pass_obj
def dagster(obj):  # pragma: no cover
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
def create(obj, file, name=None, tags=None, user_namespace=None,  # pragma: no cover
           with_decorators=None, workflow_timeout=None, deployer_attribute_file=None):
    job_name = _resolve_job_name(name, obj.flow.name, obj.flow)

    if file is None:
        # Use the job name (not just flow name) to avoid collisions when the same
        # flow is compiled multiple times with different configs/project names.
        file = f"{job_name.lower()}_dagster.py"
    if os.path.abspath(sys.argv[0]) == os.path.abspath(file):
        raise MetaflowException(
            "Dagster output file cannot be the same as the flow file."
        )

    _validate_workflow(obj.flow, obj.graph)

    obj.echo(
        f"Compiling *{obj.flow.name}* to Dagster job *{job_name}*...",
        bold=True,
    )

    flow_file = os.path.abspath(sys.argv[0])

    step_env = _gather_step_env()
    # Embed METAFLOW_FLOW_CONFIG_VALUE so step subprocesses have access to config
    # overrides (--config-value / --config) at task runtime. This is required for
    # config_expr / @project decorators to evaluate correctly in step subprocesses.
    try:
        from metaflow.flowspec import FlowStateItems
        flow_configs = obj.flow._flow_state.get(FlowStateItems.CONFIGS, {})
        config_env = {
            name: value
            for name, (value, _is_plain) in flow_configs.items()
            if value is not None
        }
        if config_env:
            step_env["METAFLOW_FLOW_CONFIG_VALUE"] = json.dumps(config_env)
    except Exception:
        pass

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
        step_env=step_env,
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
        # The "name" field becomes deployer.name, which the test framework uses
        # as the deployment identifier for DeployedFlow.from_deployment().
        # We embed the full JSON blob so that from_deployment() can reconstruct
        # the deployer without needing the original flow file.
        identifier = json.dumps({
            "name": job_name,
            "flow_name": obj.flow.name,
            "flow_file": flow_file,
            "definitions_file": os.path.abspath(file),
        })
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": identifier,
                    "flow_name": obj.flow.name,
                    "metadata": "{}",
                    "additional_info": {"definitions_file": os.path.abspath(file)},
                },
                f,
            )


def _ensure_dagster_home():
    """Return a DAGSTER_HOME path, creating a temporary one with SyncInMemoryRunLauncher if needed.

    If DAGSTER_HOME is already set and exists, it is returned unchanged so that
    callers can bring their own dagster.yaml (e.g. CI setups with a real daemon).
    Otherwise a temporary directory is created and configured with:
      - SQLite storage (single-process, no daemon required)
      - SyncInMemoryRunLauncher (executes runs synchronously via multiprocess executor)
      - DefaultRunCoordinator (no queue daemon required)

    Returns (dagster_home_path, cleanup_func).  The caller must invoke cleanup_func()
    when the run is complete to remove the temporary directory (if one was created).
    """
    existing = os.environ.get("DAGSTER_HOME")
    if existing and os.path.isdir(existing):
        return existing, lambda: None

    import atexit
    tmp_home = tempfile.mkdtemp(prefix="mf_dagster_home_")

    def _cleanup():
        import shutil
        shutil.rmtree(tmp_home, ignore_errors=True)

    storage_dir = os.path.join(tmp_home, "storage")
    os.makedirs(storage_dir, exist_ok=True)
    dagster_yaml = os.path.join(tmp_home, "dagster.yaml")
    with open(dagster_yaml, "w") as f:
        f.write(
            f"storage:\n"
            f"  sqlite:\n"
            f"    base_dir: {storage_dir}\n"
            f"run_launcher:\n"
            f"  module: dagster._core.launcher.sync_in_memory_run_launcher\n"
            f"  class: SyncInMemoryRunLauncher\n"
            f"run_coordinator:\n"
            f"  module: dagster._core.run_coordinator.default_run_coordinator\n"
            f"  class: DefaultRunCoordinator\n"
            f"telemetry:\n"
            f"  enabled: false\n"
        )
    atexit.register(_cleanup)
    return tmp_home, _cleanup


def _build_run_config(run_params):
    """Build a Dagster run config dict from key=value run_params strings."""
    if not run_params:
        return None
    ops_config = {}
    for kv in run_params:
        k, _, v = kv.partition("=")
        ops_config[k.strip()] = v.strip()
    # Parameters are passed as op-level config to op_start.
    return {"ops": {"op_start": {"config": ops_config}}}


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
def trigger(obj, definitions_file, job_name=None, run_params=None, deployer_attribute_file=None):  # pragma: no cover
    if definitions_file is None:
        definitions_file = f"{obj.flow.name.lower()}_dagster.py"
    definitions_file = os.path.abspath(definitions_file)
    resolved_job_name = _resolve_job_name(job_name, obj.flow.name, obj.flow)

    dagster_home, cleanup = _ensure_dagster_home()
    old_dagster_home = os.environ.get("DAGSTER_HOME")
    os.environ["DAGSTER_HOME"] = dagster_home

    try:
        from dagster import DagsterInstance
        from dagster._core.definitions.reconstruct import ReconstructableJob
        from dagster._core.execution.api import execute_job

        recon_job = ReconstructableJob.for_file(definitions_file, resolved_job_name)
        run_config = _build_run_config(run_params)

        with DagsterInstance.get() as instance:
            result = execute_job(
                recon_job,
                instance=instance,
                run_config=run_config,
            )

        if not result.success:
            # Collect failure messages for a helpful error
            failure_msgs = []
            for event in result.all_events:
                if event.is_failure:
                    esd = getattr(event, "event_specific_data", None)
                    err = getattr(esd, "error", None)
                    if err:
                        failure_msgs.append(str(err.message))
            detail = "; ".join(failure_msgs) if failure_msgs else "(no details)"
            raise DagsterException(
                f"Dagster job {resolved_job_name!r} failed. {detail}"
            )

        dagster_run_id = result.run_id
    finally:
        if old_dagster_home is None:
            os.environ.pop("DAGSTER_HOME", None)
        else:
            os.environ["DAGSTER_HOME"] = old_dagster_home

    # Derive the deterministic Metaflow run-id from the Dagster UUID.
    # This matches _make_run_id() in the generated definitions file.
    run_id = "dagster-" + hashlib.sha1(dagster_run_id.encode()).hexdigest()[:12]

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
def resume(  # pragma: no cover
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

        dagster_home, _cleanup = _ensure_dagster_home()
        old_dagster_home = os.environ.get("DAGSTER_HOME")
        os.environ["DAGSTER_HOME"] = dagster_home
        try:
            from dagster import DagsterInstance
            from dagster._core.definitions.reconstruct import ReconstructableJob
            from dagster._core.execution.api import execute_job

            recon_job = ReconstructableJob.for_file(resume_defs_file, resolved_job_name)

            with DagsterInstance.get() as instance:
                result_obj = execute_job(recon_job, instance=instance)

            if not result_obj.success:
                failure_msgs = []
                for event in result_obj.all_events:
                    if event.is_failure:
                        esd = getattr(event, "event_specific_data", None)
                        err = getattr(esd, "error", None)
                        if err:
                            failure_msgs.append(str(err.message))
                detail = "; ".join(failure_msgs) if failure_msgs else "(no details)"
                raise DagsterException(
                    f"Dagster job {resolved_job_name!r} resume failed. {detail}"
                )
            dagster_run_uuid = result_obj.run_id
        finally:
            if old_dagster_home is None:
                os.environ.pop("DAGSTER_HOME", None)
            else:
                os.environ["DAGSTER_HOME"] = old_dagster_home
    finally:
        try:
            os.unlink(resume_defs_file)
        except Exception:
            pass

    # Derive the new Metaflow run-id from the Dagster UUID.
    new_run_id = "dagster-" + hashlib.sha1(dagster_run_uuid.encode()).hexdigest()[:12]

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
