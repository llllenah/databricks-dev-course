from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import functions as F

from orders_dq.quality import checks as c
from orders_dq.quality.dqx import check_results, load_checks
from test_suite import build_medallion

dqx_engine = pytest.importorskip("databricks.labs.dqx.engine")

CHECKS_DIR = Path(__file__).resolve().parents[1] / "checks"
CHECK_FILES = sorted(CHECKS_DIR.glob("*.yml"))


@pytest.fixture(scope="module")
def engine(spark):
    # DQEngineCore only pings the workspace client on start, a mock is enough for local Spark.
    return dqx_engine.DQEngineCore(MagicMock(), spark)


@pytest.fixture(scope="module")
def medallion(spark):
    return build_medallion(spark)


def _refs(t):
    return {"dim_customer": t.dim_customer, "dim_date": t.dim_date, "dim_time": t.dim_time}


def _by_name(results):
    return {r.check_name: r for r in results}


def test_there_is_a_check_file_per_checked_table():
    assert {p.stem for p in CHECK_FILES} == {"orders_silver", "fact_orders"}


@pytest.mark.parametrize("path", CHECK_FILES, ids=lambda p: p.name)
def test_check_files_are_valid(path):
    checks = load_checks(path)
    status = dqx_engine.DQEngineCore.validate_checks(checks)
    assert not status.has_errors, str(status)
    names = [x["name"] for x in checks]
    assert len(names) == len(set(names))
    assert all((x.get("user_metadata") or {}).get("dimension") for x in checks)


def test_check_results_counts_rows_once_per_check(spark):
    item = "struct<name:string,message:string>"
    flagged = spark.createDataFrame(
        [
            (1, [("not_null", "a is null"), ("not_null", "b is null")], None),
            (2, [("in_range", "x")], [("per_day", "y")]),
            (3, None, None),
        ],
        f"id int, _errors array<{item}>, _warnings array<{item}>",
    )
    checks = [
        {"name": "not_null", "criticality": "error", "check": {"function": "is_not_null"},
         "user_metadata": {"dimension": "completeness"}},
        {"name": "in_range", "check": {"function": "is_in_range"}},
        {"name": "per_day", "criticality": "warn", "check": {"function": "is_aggr_not_greater_than"}},
        {"name": "unique", "criticality": "error", "check": {"function": "is_unique"}},
    ]
    res = _by_name(check_results(flagged, checks, "gold", "t"))
    assert res["dqx_not_null"].failed_rows == 1
    assert res["dqx_not_null"].total_rows == 3
    assert res["dqx_not_null"].dimension == "completeness"
    assert res["dqx_not_null"].status == c.FAIL
    assert res["dqx_in_range"].dimension == "validity"
    assert res["dqx_in_range"].status == c.FAIL
    assert res["dqx_per_day"].status == c.WARN
    assert not res["dqx_per_day"].blocking
    assert res["dqx_unique"].status == c.PASS


def test_dqx_passes_on_consistent_medallion(engine, medallion):
    for table, df, refs in (
        ("orders_silver", medallion.silver, {}),
        ("fact_orders", medallion.fact, _refs(medallion)),
    ):
        checks = load_checks(CHECKS_DIR / f"{table}.yml")
        flagged = engine.apply_checks_by_metadata(df, checks, ref_dfs=refs)
        failed = [r for r in check_results(flagged, checks, "x", table) if r.status != c.PASS]
        assert failed == [], failed


def test_dqx_finds_broken_fact_rows(spark, engine, medallion):
    fact = medallion.fact
    first = fact.orderBy("order_id").limit(1)
    broken = (
        fact.unionByName(first)  # duplicate order_id
        .unionByName(first.withColumn("order_id", F.lit(-1)).withColumn("customer_key", F.lit(9999)))
        .unionByName(first.withColumn("order_id", F.lit(-2)).withColumn("time_key", F.lit(None).cast("int")))
    )
    checks = load_checks(CHECKS_DIR / "fact_orders.yml")
    flagged = engine.apply_checks_by_metadata(broken, checks, ref_dfs=_refs(medallion))
    res = _by_name(check_results(flagged, checks, "gold", "fact_orders"))

    assert res["dqx_fact_order_id_unique"].failed_rows == 2
    assert res["dqx_fact_customer_key_in_dim_customer"].failed_rows == 1
    assert res["dqx_fact_keys_not_null"].failed_rows == 1
    assert res["dqx_fact_date_key_in_dim_date"].status == c.PASS
    assert res["dqx_fact_orders_per_customer_day"].status == c.PASS
