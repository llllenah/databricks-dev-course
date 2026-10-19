import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Optional, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs, pipelines

from .report import log

T = TypeVar("T")

RUN_DONE = {jobs.RunLifeCycleState.TERMINATED, jobs.RunLifeCycleState.SKIPPED, jobs.RunLifeCycleState.INTERNAL_ERROR}
PIPELINE_DONE = {pipelines.UpdateInfoState.COMPLETED, pipelines.UpdateInfoState.FAILED, pipelines.UpdateInfoState.CANCELED}


@dataclass
class Outcome:
    status: str
    details: str = ""
    url: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "SUCCESS"


def poll(fetch: Callable[[], T], done: Callable[[T], bool], describe: Callable[[T], str],
         timeout: timedelta, interval: float = 15.0) -> T:
    deadline = time.monotonic() + timeout.total_seconds()
    last = None
    while True:
        value = fetch()
        state = describe(value)
        if state != last:
            log(f"  {state}")
            last = state
        if done(value):
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"still {state} after {timeout}")
        time.sleep(interval)


def create_cluster(w: WorkspaceClient, name: str, node_type: Optional[str] = None, runtime: Optional[str] = None,
                   autotermination_minutes: int = 20, timeout: timedelta = timedelta(minutes=20)) -> compute.ClusterDetails:
    node_type = node_type or w.clusters.select_node_type(local_disk=True, min_cores=4)
    runtime = runtime or w.clusters.select_spark_version(long_term_support=True, scala="2.13")
    log(f"  {node_type}, {runtime}")
    return w.clusters.create(
        cluster_name=name,
        spark_version=runtime,
        node_type_id=node_type,
        kind=compute.Kind.CLASSIC_PREVIEW,
        is_single_node=True,
        data_security_mode=compute.DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
        single_user_name=w.current_user.me().user_name,
        autotermination_minutes=autotermination_minutes,
        custom_tags={"owner": "dbx-ops"},
    ).result(timeout=timeout)


def delete_cluster(w: WorkspaceClient, cluster_id: str) -> None:
    w.clusters.permanent_delete(cluster_id)


def submit_notebook(w: WorkspaceClient, cluster_id: str, path: str, params: dict[str, str], run_name: str) -> int:
    return w.jobs.submit(
        run_name=run_name,
        tasks=[
            jobs.SubmitTask(
                task_key="notebook",
                existing_cluster_id=cluster_id,
                notebook_task=jobs.NotebookTask(notebook_path=path, base_parameters=params),
            )
        ],
    ).run_id


def run_job(w: WorkspaceClient, job_id: int, params: Optional[dict[str, str]] = None) -> int:
    return w.jobs.run_now(job_id=job_id, job_parameters=params or None).run_id


def wait_run(w: WorkspaceClient, run_id: int, timeout: timedelta = timedelta(hours=1)) -> Outcome:
    def describe(run: jobs.Run) -> str:
        state = run.state.result_state or run.state.life_cycle_state
        tasks = ", ".join(f"{t.task_key}={(t.state.result_state or t.state.life_cycle_state).value}"
                          for t in run.tasks or [] if t.state)
        return f"{state.value}" + (f" [{tasks}]" if tasks else "")

    run = poll(lambda: w.jobs.get_run(run_id), lambda r: r.state.life_cycle_state in RUN_DONE, describe, timeout)
    result = run.state.result_state.value if run.state.result_state else run.state.life_cycle_state.value
    failed = [t.task_key for t in run.tasks or [] if t.state and t.state.result_state
              and t.state.result_state != jobs.RunResultState.SUCCESS]
    details = f"failed tasks: {', '.join(failed)}" if failed else (run.state.state_message or "")
    return Outcome(result, details, run.run_page_url or "")


def start_pipeline(w: WorkspaceClient, pipeline_id: str, full_refresh: bool = False) -> str:
    return w.pipelines.start_update(pipeline_id, full_refresh=full_refresh).update_id


def wait_pipeline(w: WorkspaceClient, pipeline_id: str, update_id: str,
                  timeout: timedelta = timedelta(minutes=45)) -> Outcome:
    update = poll(
        lambda: w.pipelines.get_update(pipeline_id, update_id).update,
        lambda u: u.state in PIPELINE_DONE,
        lambda u: u.state.value,
        timeout,
    )
    status = "SUCCESS" if update.state == pipelines.UpdateInfoState.COMPLETED else update.state.value
    url = f"{w.config.host}/pipelines/{pipeline_id}/updates/{update_id}"
    return Outcome(status, f"update {update_id}", url)


def find_job_id(w: WorkspaceClient, name: str) -> int:
    matches = [j.job_id for j in w.jobs.list(name=name)]
    if len(matches) != 1:
        raise LookupError(f"expected one job named {name!r}, found {len(matches)}")
    return matches[0]


def find_pipeline_id(w: WorkspaceClient, name: str) -> str:
    matches = [p.pipeline_id for p in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'")]
    if len(matches) != 1:
        raise LookupError(f"expected one pipeline named {name!r}, found {len(matches)}")
    return matches[0]
