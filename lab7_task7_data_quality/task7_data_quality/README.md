# Lab 7 — Data Quality Testing & Unit Tests

## Overview

This lab adds tests to the orders medallion from Lab 6. Unit tests check the transformation code.
Data quality tests check the data in every layer and act as a gate: the job fails when a blocking
check fails, and the CI workflow fails with it.

Catalog: `dbr_dev_ua5816bd` (course DEV workspace, default target). Schemas:
`lena066636_dq_bronze`, `lena066636_dq_silver`, `lena066636_dq_gold`. Lab 7 uses its own schemas,
so the Lab 6 tables and dashboard are not changed.

## Architecture

```
landing volume (JSON) ─▶ bronze.orders_bronze ─▶ silver.orders_silver ─▶ gold: dim_customer, fact_orders,
                                              └▶ silver.orders_quarantine     agg_daily_sales, agg_channel_sales
                                                                        gold: dim_date, dim_time (static)
```

Job `lena066636-lab7-orders-dq` (Asset Bundle):

```
unit_tests ─▶ generate_landing_data ─▶ orders_pipeline ─┐
          └▶ static_dimensions ─────────────────────────┴▶ build_gold ─▶ delta_constraints ─▶ dq_suite
```

## Project structure

| Path | Content |
|---|---|
| `src/orders_dq/transformations.py` | Pure transformation functions: parsing, keys, day part, dimensions, fact, aggregations |
| `src/orders_dq/quality/rules.py` | Silver rules shared by the pipeline expectations and the tests, row flagging, deduplication |
| `src/orders_dq/quality/checks.py` | Check functions for completeness, uniqueness, validity, consistency and timeliness |
| `src/orders_dq/quality/reconciliation.py` | Row count and amount reconciliation between layers |
| `src/orders_dq/quality/suite.py` | The suite across the medallion, results table, scorecard and the gate |
| `src/orders_dq/gold_ddl.py` | Gold table DDL with NOT NULL and CHECK constraints |
| `src/orders_dq/generator.py` | Test data: 1000 valid orders and 30 bad records of 10 kinds |
| `pipelines/orders_pipeline.py` | Lakeflow declarative pipeline: bronze, silver, quarantine |
| `notebooks/` | Job tasks: data generation, static dimensions, gold, constraint tests, DQ suite, quarantine review, test runner |
| `tests/` | pytest unit tests (52 tests) |
| `resources/`, `databricks.yml` | Asset Bundle: pipeline, jobs, permissions, targets `trial` and `dev` |
| `sql/dq_scorecard.sql` | Queries for a DQ scorecard dashboard and an alert |
| `sample_data/` | The same generated batches as plain JSON files |
| `../.github/workflows/task7-data-quality.yml` | CI: unit tests, then deploy and run the DQ job as a gate |

## Part A — Unit tests

The transformation logic lives in the `orders_dq` package instead of notebook cells. Notebooks and
the pipeline only import it. The tests cover:

- timestamp parsing in several formats, with invalid values turned into nulls instead of errors;
- type casting and normalization of order fields;
- `date_key`, `time_key`, day part and amount bucket boundaries;
- `dim_date` and `dim_time` generation;
- the spend-based segment (premium is the top fifth of customers);
- the fact table: every order gets all keys, and there is no raw `order_ts` column;
- the silver rules: every kind of bad record is flagged by the expected rule, duplicates keep the first record;
- every check function and the reconciliation, including cases that must fail;
- the whole suite on a consistent medallion and on a broken one (lost rows, orphan keys, high quarantine share).

The `spark` fixture in `tests/conftest.py` picks the session:

| `SPARK_MODE` | Session |
|---|---|
| `local` | local PySpark, used in CI |
| `connect` | Databricks Connect, remote serverless compute (or `DATABRICKS_CLUSTER_ID` if set) |
| `databricks` | the notebook session, used by the `run_unit_tests` job task |
| `auto` (default) | notebook session, then Databricks Connect if `DATABRICKS_HOST` is set, then local |

Run locally:

```bash
cd task7_data_quality
pip install -e ".[local]"
SPARK_MODE=local pytest
```

Run locally against remote Spark with Databricks Connect:

```bash
pip install -e ".[connect]"
databricks auth login --host https://adb-7405604651938730.10.azuredatabricks.net --profile dev
export DATABRICKS_CONFIG_PROFILE=dev
SPARK_MODE=connect pytest
```

`databricks-connect` must match the compute version. With serverless compute, version 17 needs
Python 3.12. The same setup lets you run and debug the modules from the IDE.

Run headless in the workspace with the bundle:

```bash
databricks bundle deploy -t dev
databricks bundle run -t dev lab7_unit_tests
```

## Part B — Data quality testing

### In-pipeline gates (Lakeflow expectations)

| Table | Expectation | Action |
|---|---|---|
| `orders_bronze` | `source_file_present` | fail the update |
| `orders_silver` | `order_id_not_null`, `customer_not_null` | drop |
| `orders_silver` | `order_ts_parsed`, `amount_in_range` (0.01 to 10000), `channel_accepted` (web, mobile, store), `customer_format` (`cust_<n>`) | drop |
| `orders_silver` | `order_ts_not_in_future` | drop |

The same rules flag every bronze row in the temporary view `orders_validated`. A row that fails
any rule, or that repeats an `order_id` already taken, goes to `orders_quarantine` with the list of
failed rules in `dq_failed_rules`. Every bronze row ends up in exactly one of silver or quarantine.

Expected result with the generated data: bronze 1030, silver 1000, quarantine 30.

| Rule | Quarantined rows |
|---|---|
| `amount_in_range` | 9 (negative, too large, not numeric) |
| `customer_format` | 6 (wrong format, null) |
| `order_ts_not_in_future` | 6 (future date, unparsable date) |
| `customer_not_null` | 3 |
| `order_id_not_null` | 3 |
| `channel_accepted` | 3 |
| `order_ts_parsed` | 3 |
| `unique_order_id` | 3 |

A row can fail more than one rule, so the sum is larger than 30. `05_inspect_quarantine` shows the
breakdown and the rejected rows.

### Delta constraints

Gold tables are created from DDL with `NOT NULL` columns and `CHECK` constraints (amount range,
hour range, date key format, accepted channels and segments, positive aggregates). Data is written
with `INSERT OVERWRITE`, so the constraints stay on the tables. `03_delta_constraints` tries five
invalid inserts into `fact_orders` and expects each one to be rejected. A write that gets through
fails the task.

### Data quality suite

`04_dq_suite` runs the suite across all layers and appends the results to `gold.dq_results`.

| Dimension | Checks |
|---|---|
| Completeness | required columns not null in bronze, silver and fact; `dim_time` has 24 hours |
| Uniqueness | `order_id` in silver and fact; keys of `dim_customer`, `dim_date`, `dim_time` |
| Validity | amount range, accepted channels and segments, customer format, hour range, quarantine share up to 5% |
| Consistency | fact keys exist in every dimension; `date_key` matches `date`; fact segment matches `dim_customer` |
| Timeliness | latest ingestion and latest order within 48 hours (warning, not blocking) |
| Reconciliation | bronze = silver + quarantine; silver = fact; fact = sum of daily orders; silver amount = fact amount = daily revenue |

Each check gets PASS, WARN or FAIL. Any blocking FAIL raises `DataQualityError`, so the task, the
job and the CI run fail. The view `gold.dq_scorecard_latest` shows the score by layer and
dimension for the latest run, and `sql/dq_scorecard.sql` has queries for a dashboard and an alert.
The job sends an email on failure.

## CI

The workflow `.github/workflows/task7-data-quality.yml` runs on changes in `task7_data_quality/`:

1. `unit-tests`: installs the package with PySpark and runs pytest.
2. `dq-gate` (after unit tests pass, on push to `main` or manual run): validates and deploys the
   bundle to the DEV workspace, then runs the job. The pipeline, the constraint tests and the
   DQ suite run there, and a failed blocking check fails the workflow.

The gate needs two repository secrets: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
(Settings → Secrets and variables → Actions).

## Deployment

```bash
cd task7_data_quality
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev lab7_orders_dq
```

All parameters come from bundle variables, and nothing is hardcoded in the code. The `trial` target
deploys the same project to the trial workspace with catalog `dbr_dev_ua_5816_trail`.

The job and the pipeline are shared with the workspace `users` group (CAN_VIEW). The gold notebook
grants `USE SCHEMA` and `SELECT` on the silver and gold schemas to `account users`, so reviewers
can open the pipeline, the job runs and the data.

## Notes from previous reviews

- `dim_date` is loaded once for several years in a separate task and is not rebuilt with the facts.
- `dim_time` holds the 24 hours with the day part. The fact table keeps `date_key` and `time_key`
  only, with no high-cardinality timestamp column. A unit test checks this.
- The whole lab is deployed as a pipeline and a job, so it is visible in Jobs & Pipelines.
- The lab runs in the course DEV workspace, where reviewers already have access.
- Read access for reviewers is granted on the job, the pipeline and the schemas.
