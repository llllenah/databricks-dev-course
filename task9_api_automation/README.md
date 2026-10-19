# Lab 9 — Databricks REST API automation

`dbx-ops` is a small platform-management CLI on the Databricks Python SDK. It provisions compute,
runs notebooks and jobs, triggers pipelines, follows them to the end and reports the result. It works
on the resources deployed by the Lab 8 bundle and is part of the same CI/CD flow.

## Commands

| Command | API calls |
|---|---|
| `cluster create [--node-type] [--autotermination]` | `clusters.create`: single-node, dedicated access mode, auto-termination; waits until RUNNING |
| `cluster delete <id>` | `clusters.permanent_delete` |
| `notebook --cluster-id --path [--param k=v]` | `jobs.submit` one-time run on the cluster, then `jobs.get_run` until it ends |
| `pipeline (--id \| --name) [--full-refresh]` | `pipelines.start_update`, then `pipelines.get_update` until it ends |
| `job (--id \| --name) [--param k=v]` | `jobs.run_now`, then `jobs.get_run` with the state of every task |
| `e2e --bundle-summary <file> [--keep-cluster]` | the whole scenario below |

Every command prints state changes while it waits and ends with a report table. With `--report-json`
the report is also saved as JSON, and in GitHub Actions it goes to the job summary. The exit code is
`1` if any step failed, so the CLI can gate a pipeline.

## End-to-end scenario

```
provision cluster ─▶ notebook on the cluster ─▶ pipeline update ─▶ job orders-gold-dq ─▶ delete cluster
                     (generate landing data)    (bronze, silver)    (gold + DQ checks)    (always)
```

Resource IDs come from `databricks bundle summary -o json`, so nothing is hardcoded. The cluster is
deleted even when a step fails.

`notebooks/platform_e2e.ipynb` runs the same scenario from a Databricks Git folder: it finds the
pipeline and the job by name and shows the report as a table.

## CI/CD

| Workflow | What runs `dbx-ops` |
|---|---|
| `task8-cicd.yml`, merge to `main` | after `bundle deploy -t prod`: `dbx-ops job` runs `orders-daily` and gates the deployment |
| `task9-platform-ops.yml` | unit tests on pull requests; `dbx-ops e2e` on PROD after every successful Lab 8 deployment and on manual runs |

The e2e job shares the concurrency group with the Lab 8 deployment, so they never overlap.

## Local use

```bash
cd task9_api_automation
pip install -e ".[test]"
pytest

databricks bundle summary -t dev -o json > bundle.json   # from task8_cicd
dbx-ops --profile <profile> e2e --bundle-summary bundle.json
dbx-ops --profile <profile> job --name "[dev lena066636] orders-daily"
```

Authentication follows the SDK: `--profile` from `~/.databrickscfg`, or `DATABRICKS_HOST` and
`DATABRICKS_TOKEN` in CI.
