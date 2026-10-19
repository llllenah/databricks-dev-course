import argparse
import os
import sys
import time
from typing import Callable, Optional

from databricks.sdk import WorkspaceClient

from . import ops
from .bundle import OrdersBundle, load_bundle
from .report import Report


def run_step(report: Report, name: str, action: Callable[[], ops.Outcome]) -> bool:
    step = report.start(name)
    try:
        outcome = action()
    except Exception as e:
        report.finish(step, "FAILED", f"{type(e).__name__}: {e}")
        return False
    report.finish(step, outcome.status, outcome.details, outcome.url)
    return outcome.ok


def e2e(w: WorkspaceClient, bundle: OrdersBundle, node_type: Optional[str] = None,
        keep_cluster: bool = False) -> Report:
    report = Report("Platform e2e")
    cluster: dict[str, str] = {}

    def provision() -> ops.Outcome:
        details = ops.create_cluster(w, f"dbx-ops-e2e-{time.strftime('%Y%m%d-%H%M%S')}", node_type)
        cluster["id"] = details.cluster_id
        return ops.Outcome("SUCCESS", f"{details.cluster_id}, {details.node_type_id}, {details.spark_version}",
                           f"{w.config.host}/compute/clusters/{details.cluster_id}")

    try:
        steps = [
            ("provision cluster", provision),
            ("notebook on cluster: generate landing data", lambda: ops.wait_run(w, ops.submit_notebook(
                w, cluster["id"], bundle.landing_notebook, bundle.landing_params(), "dbx-ops: generate landing data"))),
            ("pipeline update: bronze and silver", lambda: ops.wait_pipeline(
                w, bundle.pipeline_id, ops.start_pipeline(w, bundle.pipeline_id, full_refresh=True))),
            ("job: orders_gold_dq", lambda: ops.wait_run(w, ops.run_job(w, bundle.jobs["orders_gold_dq"]))),
        ]
        for name, action in steps:
            if not run_step(report, name, action):
                break
    finally:
        if "id" in cluster and not keep_cluster:
            run_step(report, "delete cluster", lambda: ops.delete_cluster(w, cluster["id"]) or ops.Outcome("SUCCESS"))
    return report


def parse_params(values: list[str]) -> dict[str, str]:
    params = {}
    for value in values:
        key, sep, val = value.partition("=")
        if not sep or not key:
            raise argparse.ArgumentTypeError(f"expected key=value, got {value!r}")
        params[key] = val
    return params


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dbx-ops", description="Databricks platform operations")
    parser.add_argument("--profile", help="Databricks CLI profile; environment variables are used when omitted")
    parser.add_argument("--report-json", help="also write the report to this JSON file")
    sub = parser.add_subparsers(dest="command", required=True)

    cluster = sub.add_parser("cluster", help="create or delete a single-node cluster")
    cluster_sub = cluster.add_subparsers(dest="action", required=True)
    create = cluster_sub.add_parser("create")
    create.add_argument("--name", default=f"dbx-ops-{time.strftime('%Y%m%d-%H%M%S')}")
    create.add_argument("--node-type")
    create.add_argument("--autotermination", type=int, default=20, help="minutes")
    delete = cluster_sub.add_parser("delete")
    delete.add_argument("cluster_id")

    notebook = sub.add_parser("notebook", help="run a notebook on a cluster as a one-time job run")
    notebook.add_argument("--cluster-id", required=True)
    notebook.add_argument("--path", required=True)
    notebook.add_argument("--param", action="append", default=[], help="key=value, repeatable")

    pipeline = sub.add_parser("pipeline", help="start a pipeline update and wait for it")
    target = pipeline.add_mutually_exclusive_group(required=True)
    target.add_argument("--id")
    target.add_argument("--name")
    pipeline.add_argument("--full-refresh", action="store_true")

    job = sub.add_parser("job", help="run a job and wait for it")
    target = job.add_mutually_exclusive_group(required=True)
    target.add_argument("--id", type=int)
    target.add_argument("--name")
    job.add_argument("--param", action="append", default=[], help="key=value, repeatable")

    flow = sub.add_parser("e2e", help="cluster, notebook, pipeline, job and cleanup in one go")
    flow.add_argument("--bundle-summary", required=True, help="output of `databricks bundle summary -o json`")
    flow.add_argument("--node-type")
    flow.add_argument("--keep-cluster", action="store_true")
    return parser


def dispatch(w: WorkspaceClient, args: argparse.Namespace) -> Report:
    if args.command == "e2e":
        return e2e(w, load_bundle(args.bundle_summary), args.node_type, args.keep_cluster)

    report = Report(f"dbx-ops {args.command}")
    if args.command == "cluster" and args.action == "create":
        def create() -> ops.Outcome:
            c = ops.create_cluster(w, args.name, args.node_type, autotermination_minutes=args.autotermination)
            return ops.Outcome("SUCCESS", c.cluster_id, f"{w.config.host}/compute/clusters/{c.cluster_id}")
        run_step(report, f"create cluster {args.name}", create)
    elif args.command == "cluster":
        run_step(report, f"delete cluster {args.cluster_id}",
                 lambda: ops.delete_cluster(w, args.cluster_id) or ops.Outcome("SUCCESS"))
    elif args.command == "notebook":
        params = parse_params(args.param)
        run_step(report, f"notebook {args.path}", lambda: ops.wait_run(
            w, ops.submit_notebook(w, args.cluster_id, args.path, params, f"dbx-ops: {args.path}")))
    elif args.command == "pipeline":
        def pipeline() -> ops.Outcome:
            pipeline_id = args.id or ops.find_pipeline_id(w, args.name)
            return ops.wait_pipeline(w, pipeline_id, ops.start_pipeline(w, pipeline_id, args.full_refresh))
        run_step(report, f"pipeline {args.id or args.name}", pipeline)
    elif args.command == "job":
        params = parse_params(args.param)
        run_step(report, f"job {args.id or args.name}", lambda: ops.wait_run(
            w, ops.run_job(w, args.id or ops.find_job_id(w, args.name), params)))
    return report


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    report = dispatch(w, args)

    markdown = report.to_markdown()
    print("\n" + markdown)
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(markdown)
    if args.report_json:
        with open(args.report_json, "w", encoding="utf-8") as f:
            f.write(report.to_json())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
