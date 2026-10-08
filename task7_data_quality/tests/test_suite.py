from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from orders_dq import generator, transformations as t
from orders_dq.quality import checks as c
from orders_dq.quality.rules import flag_violations
from orders_dq.quality.suite import (
    DataQualityError,
    MedallionTables,
    gate,
    results_to_df,
    run_suite,
    scorecard,
    summarize,
)

NOW = datetime(2026, 10, 6, 12, 0, 0)


def build_medallion(spark, n_valid=300, bad_per_kind=1):
    rows = generator.generate_orders(n_valid=n_valid, bad_per_kind=bad_per_kind, end=NOW)
    bronze = spark.createDataFrame(
        [(x["order_id"], x["customer"], x["channel"], x["amount"], x["ts"]) for x in rows], t.RAW_ORDER_SCHEMA
    ).withColumn("_source_file", F.lit("batch_1.json")).withColumn("_ingested_at", F.lit(NOW))
    flagged = flag_violations(t.parse_orders(bronze))
    silver = flagged.where("dq_is_valid").drop("dq_failed_rules", "_dup_rank", "dq_is_valid")
    quarantine = flagged.where("NOT dq_is_valid")
    dim_customer = t.build_dim_customer(silver)
    fact = t.build_fact_orders(silver, dim_customer)
    return MedallionTables(
        bronze=bronze,
        silver=silver,
        quarantine=quarantine,
        dim_customer=dim_customer,
        dim_date=t.build_dim_date(spark, date(2026, 1, 1), date(2026, 12, 31)),
        dim_time=t.build_dim_time(spark),
        fact=fact,
        agg_daily=t.build_agg_daily_sales(fact),
    )


@pytest.fixture(scope="module")
def medallion(spark):
    return build_medallion(spark)


def test_suite_passes_on_consistent_medallion(medallion):
    results = run_suite(medallion, now=NOW)
    failed = [r for r in results if r.status != c.PASS]
    assert failed == [], failed
    assert {r.dimension for r in results} >= {"completeness", "uniqueness", "validity", "consistency",
                                               "timeliness", "reconciliation"}
    gate(results)


def test_suite_detects_silent_data_loss(spark, medallion):
    broken = MedallionTables(**{**medallion.__dict__, "fact": medallion.fact.limit(medallion.fact.count() - 5)})
    results = run_suite(broken, now=NOW)
    names = {r.check_name for r in results if r.status == c.FAIL}
    assert {"silver_eq_fact", "silver_amount_eq_fact_amount"} <= names
    with pytest.raises(DataQualityError, match="silver_eq_fact"):
        gate(results)


def test_suite_detects_orphan_keys(spark, medallion):
    bad_fact = medallion.fact.withColumn(
        "customer_key", F.when(F.col("order_id") == 1, F.lit(9999)).otherwise(F.col("customer_key"))
    )
    results = run_suite(MedallionTables(**{**medallion.__dict__, "fact": bad_fact}), now=NOW)
    assert any(r.check_name == "customer_key_exists_in_dim_customer" and r.status == c.FAIL for r in results)


def test_high_quarantine_ratio_blocks(spark):
    med = build_medallion(spark, n_valid=100, bad_per_kind=2)
    results = run_suite(med, now=NOW)
    ratio = next(r for r in results if r.check_name == "quarantine_ratio")
    assert ratio.status == c.FAIL


def test_results_dataframe_and_scorecard(spark, medallion):
    results = run_suite(medallion, now=NOW)
    df = results_to_df(spark, results, run_id="test", run_ts=NOW)
    assert df.count() == len(results)
    card = scorecard(df)
    assert card.agg(F.sum("checks")).first()[0] == len(results)
    assert summarize(results)["failed"] == 0
