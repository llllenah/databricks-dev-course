import json
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service import compute, jobs, pipelines

from dbx_ops import cli, ops
from dbx_ops.bundle import load_bundle

SUMMARY = {
    "workspace": {"file_path": "/Workspace/Users/me/.bundle/lab8/prod/files"},
    "variables": {"catalog": {"value": "cat"}},
    "resources": {
        "schemas": {"bronze": {"name": "p_orders_bronze"}},
        "volumes": {"landing": {"name": "landing"}},
        "pipelines": {"orders_pipeline": {"id": "pipe-1"}},
        "jobs": {"orders_gold_dq": {"id": "42"}, "orders_daily": {"id": "7"}},
    },
}


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(ops.time, "sleep", lambda _: None)


@pytest.fixture
def summary_file(tmp_path):
    path = tmp_path / "bundle.json"
    path.write_text(json.dumps(SUMMARY), encoding="utf-8")
    return str(path)


def run(state, result=None, tasks=()):
    return jobs.Run(
        run_id=1,
        run_page_url="https://host/run/1",
        state=jobs.RunState(life_cycle_state=state, result_state=result),
        tasks=[jobs.RunTask(task_key=k, state=jobs.RunState(life_cycle_state=state, result_state=r)) for k, r in tasks],
    )


def test_parse_params():
    assert cli.parse_params(["a=1", "b=x=y", "c="]) == {"a": "1", "b": "x=y", "c": ""}
    with pytest.raises(Exception):
        cli.parse_params(["novalue"])


def test_load_bundle(summary_file):
    b = load_bundle(summary_file)
    assert b.pipeline_id == "pipe-1"
    assert b.jobs == {"orders_gold_dq": 42, "orders_daily": 7}
    assert b.landing_notebook.endswith("/files/notebooks/00_generate_landing_data")
    assert b.landing_params() == {
        "catalog": "cat",
        "bronze_schema": "p_orders_bronze",
        "landing_volume": "landing",
        "src_path": "/Workspace/Users/me/.bundle/lab8/prod/files/src",
    }


def test_wait_run_reports_failed_tasks():
    w = MagicMock()
    w.jobs.get_run.side_effect = [
        run(jobs.RunLifeCycleState.PENDING),
        run(jobs.RunLifeCycleState.RUNNING, tasks=[("a", None), ("b", None)]),
        run(jobs.RunLifeCycleState.TERMINATED, jobs.RunResultState.FAILED,
            tasks=[("a", jobs.RunResultState.SUCCESS), ("b", jobs.RunResultState.FAILED)]),
    ]
    outcome = ops.wait_run(w, 1)
    assert outcome.status == "FAILED"
    assert outcome.details == "failed tasks: b"
    assert outcome.url == "https://host/run/1"


def test_wait_pipeline_success():
    w = MagicMock()
    w.config.host = "https://host"
    states = [pipelines.UpdateInfoState.RUNNING, pipelines.UpdateInfoState.COMPLETED]
    w.pipelines.get_update.side_effect = [MagicMock(update=pipelines.UpdateInfo(state=s)) for s in states]
    outcome = ops.wait_pipeline(w, "pipe-1", "upd-1")
    assert outcome.ok
    assert outcome.url == "https://host/pipelines/pipe-1/updates/upd-1"


def test_poll_times_out():
    with pytest.raises(TimeoutError):
        ops.poll(lambda: "RUNNING", lambda s: False, str, timedelta(seconds=-1))


def fake_platform(monkeypatch, pipeline_status="SUCCESS"):
    calls = []
    monkeypatch.setattr(ops, "create_cluster", lambda w, name, node_type: calls.append("create") or
                        compute.ClusterDetails(cluster_id="c-1", node_type_id="n", spark_version="17.3"))
    monkeypatch.setattr(ops, "submit_notebook", lambda w, cid, path, params, name: calls.append(("notebook", cid)) or 11)
    monkeypatch.setattr(ops, "start_pipeline", lambda w, pid, full_refresh: calls.append(("pipeline", pid)) or "u")
    monkeypatch.setattr(ops, "wait_pipeline", lambda w, pid, uid: ops.Outcome(pipeline_status))
    monkeypatch.setattr(ops, "run_job", lambda w, job_id: calls.append(("job", job_id)) or 12)
    monkeypatch.setattr(ops, "wait_run", lambda w, run_id: ops.Outcome("SUCCESS"))
    monkeypatch.setattr(ops, "delete_cluster", lambda w, cid: calls.append(("delete", cid)))
    return calls


def test_e2e_runs_every_step_and_cleans_up(monkeypatch, summary_file):
    calls = fake_platform(monkeypatch)
    report = cli.e2e(MagicMock(), summary_file, None, keep_cluster=False)
    assert report.ok
    assert calls == ["create", ("notebook", "c-1"), ("pipeline", "pipe-1"), ("job", 42), ("delete", "c-1")]
    assert [s.name for s in report.steps][-1] == "delete cluster"


def test_e2e_stops_on_failure_and_still_deletes_cluster(monkeypatch, summary_file):
    calls = fake_platform(monkeypatch, pipeline_status="FAILED")
    report = cli.e2e(MagicMock(), summary_file, None, keep_cluster=False)
    assert not report.ok
    assert ("job", 42) not in calls
    assert calls[-1] == ("delete", "c-1")


def test_e2e_keep_cluster(monkeypatch, summary_file):
    calls = fake_platform(monkeypatch)
    cli.e2e(MagicMock(), summary_file, None, keep_cluster=True)
    assert ("delete", "c-1") not in calls


def test_report_markdown_and_exit_code(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "WorkspaceClient", MagicMock())
    monkeypatch.setattr(ops, "find_job_id", lambda w, name: 5)
    monkeypatch.setattr(ops, "run_job", lambda w, job_id, params: 9)
    monkeypatch.setattr(ops, "wait_run", lambda w, run_id: ops.Outcome("FAILED", "failed tasks: x", "https://u"))
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    code = cli.main(["--report-json", str(tmp_path / "r.json"), "job", "--name", "orders-daily", "--param", "k=v"])

    assert code == 1
    assert "| [job orders-daily](https://u) | FAILED |" in summary.read_text(encoding="utf-8")
    assert json.loads((tmp_path / "r.json").read_text(encoding="utf-8"))["ok"] is False
