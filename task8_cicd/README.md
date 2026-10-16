# Lab 8 — CI/CD: DEV → PROD promotion

## Overview

The orders medallion from Lab 7 is expressed as one Databricks Asset Bundle with two targets and is
promoted from DEV to PROD by GitHub Actions. Everything the project needs is code in this folder:
schemas, the landing volume, grants, the pipeline, the jobs, the gold DDL, the notebooks, the dashboard,
the alert and the permissions. Nobody changes PROD by hand. A merge to `main` deploys the bundle and
runs the job.

| Target | Workspace | Catalog | Mode | Who deploys |
|---|---|---|---|---|
| `dev` (default) | course DEV `adb-7405604651938730` | `dbr_dev_ua5816bd` | development | developer, from the laptop |
| `prod` | paid Azure (trial_2) `adb-7405616780054557` | `dbr_dev_ua_5816_trail_2` | production | GitHub Actions only, on merge to `main` |

Development mode prefixes every resource (`dev_lena066636_orders_gold`, `[dev lena066636] orders-daily`),
puts the pipeline in development mode and pauses schedules, so a developer never touches shared objects.
The PROD workspace is shared with other students, so the `prod` target prefixes everything with
`lena066636` (`lena066636_orders_gold`, `[lena066636] orders-daily`). Production mode also refuses to
deploy with a pipeline in development mode or without a fixed `root_path`.

## Changes after the Lab 7 review

1. **Data loading is a separate job.** `orders-ingest` writes the landing files and runs the bronze/silver
   pipeline. `orders-gold-dq` builds gold and runs the data quality checks. `orders-daily` runs the two
   jobs one after the other (`run_job_task`) and is the job CI starts after a deploy.
2. **Unit tests are not a job task any more.** They run only in CI, before anything is deployed.
   `notebooks/run_unit_tests.py` and the `lab7_unit_tests` job are removed.
3. **DQX.** The [Databricks Labs DQX](https://github.com/databrickslabs/dqx) framework checks
   `orders_silver` and `fact_orders` with rules from YAML files in `checks/` (`05_dqx_checks`).
   Its results go to the same `gold.dq_results` table as the custom suite, so they appear on the same
   scorecard, dashboard and alert. The custom suite still runs at the end of the job, for the checks that
   are not row-level: reconciliation between layers, quarantine share and freshness.

## Jobs

```
orders-daily (schedule 06:00 Kyiv, paused by default)
├── ingest  → orders-ingest:   generate_landing_data ─▶ orders_pipeline (bronze, silver, quarantine)
└── gold_dq → orders-gold-dq:  gold_ddl ─▶ static_dimensions ─▶ build_gold ─┬▶ delta_constraints ─┬▶ dq_suite
                                                                           └▶ dqx_checks ─────────┘
```

| Task | What it does |
|---|---|
| `generate_landing_data` | 1000 valid orders and 30 bad records into the landing volume |
| `orders_pipeline` | Lakeflow declarative pipeline, full refresh: bronze, silver with expectations, quarantine |
| `gold_ddl` | gold tables with `NOT NULL` and `CHECK` constraints, `dq_results`, `dq_scorecard_latest` view |
| `static_dimensions` | `dim_date`, `dim_time`, loaded once |
| `build_gold` | `dim_customer`, `fact_orders`, `agg_daily_sales`, `agg_channel_sales` |
| `delta_constraints` | five invalid inserts that must be rejected by the constraints |
| `dqx_checks` | DQX rules on silver and fact, flagged rows into `dqx_<table>_issues` |
| `dq_suite` | custom suite and reconciliation across layers, blocking failures fail the job |

## Everything as code

| Object | Where | How it is created |
|---|---|---|
| schemas `orders_bronze`, `orders_silver`, `orders_gold` | `resources/catalog_objects.yml` | bundle `schemas`, with grants; `prevent_destroy` in PROD |
| volume `landing` | `resources/catalog_objects.yml` | bundle `volumes`, with grants |
| bronze, silver, quarantine | `pipelines/orders_pipeline.py`, `resources/pipeline.yml` | declarative pipeline |
| gold tables and view (DDL) | `src/orders_dq/gold_ddl.py`, task `gold_ddl` | `CREATE TABLE IF NOT EXISTS`, missing constraints only, `CREATE OR REPLACE VIEW` |
| jobs | `resources/jobs.yml` | bundle `jobs` |
| notebooks, Python package, DQX rules | `notebooks/`, `src/`, `checks/` | synced by the bundle |
| dashboard | `dashboards/orders_dq.lvdash.json`, `resources/dashboards_alerts.yml` | bundle `dashboards`; queries use plain table names, `dataset_catalog`/`dataset_schema` point them at the target |
| alert and its query | `resources/dashboards_alerts.yml` | bundle `alerts`: blocking DQ failures in the last day |
| permissions | `databricks.yml`, `catalog_objects.yml` | `CAN_MANAGE` for the deployer, `CAN_VIEW` for `users`; Unity Catalog grants for `account users` |

The SQL warehouse is found by name with a variable lookup, so no IDs are hardcoded.

## Idempotent deployments

- `bundle deploy` compares the code with the deployed state and only applies the difference. Running it
  twice in a row changes nothing.
- All DDL is idempotent, gold tables are written with `INSERT OVERWRITE`, the pipeline runs with full
  refresh and the landing data is regenerated, so every job run gives the same result.
- PROD is changed only by the workflow. The deploy job runs one at a time (`concurrency`), and schemas
  are protected from `bundle destroy`.

## CI/CD

`.github/workflows/task8-cicd.yml`:

| Event | Jobs |
|---|---|
| pull request | `unit-tests` (pytest on local PySpark, DQX included) ─▶ `validate` (`bundle validate` for `dev` and `prod`) |
| merge to `main` | `unit-tests` ─▶ `deploy-prod`: `bundle validate`, `bundle deploy -t prod`, `bundle run -t prod orders_daily` |

The run fails if the deploy fails or if the deployed job fails, including a blocking data quality check.
`deploy-prod` uses the GitHub environment `prod`, so every deployment is listed under *Deployments*.

Repository secrets (Settings → Secrets and variables → Actions):

| Secret | Used for |
|---|---|
| `DATABRICKS_TOKEN` | DEV workspace, validation on pull requests |
| `PROD_DATABRICKS_TOKEN` | PROD workspace, validation and deployment |

A service principal is the better identity for PROD: create it in the workspace, give it access to
the catalog, create a token for it and put that token into `PROD_DATABRICKS_TOKEN`. The workflow does not change.

## Unit tests

```bash
cd task8_cicd
pip install -e ".[local]"
SPARK_MODE=local pytest
```

Or against remote serverless compute with Databricks Connect (`pip install -e ".[connect]"`,
`SPARK_MODE=connect`). New in this lab: `tests/test_dqx.py` checks that the DQX rule files are valid,
that DQX passes on a consistent medallion and catches duplicates, orphan keys and nulls in the fact table.

## Working with the bundle

```bash
cd task8_cicd
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev orders_daily
```

The PROD target is deployed only by CI.
