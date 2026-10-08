from collections import Counter
from datetime import datetime

from pyspark.sql import functions as F

from orders_dq import generator, transformations as t
from orders_dq.quality import rules as r


def flagged_rows(spark, rows):
    df = spark.createDataFrame(rows, t.RAW_ORDER_SCHEMA).withColumn(
        "_ingested_at", F.lit(datetime(2026, 10, 6, 12, 0, 0))
    )
    return r.flag_violations(t.parse_orders(df))


def failed_for(df, order_id):
    return set(df.where(F.col("order_id") == order_id).first()["dq_failed_rules"])


def test_clean_record_passes_all_rules(spark):
    df = flagged_rows(spark, [("1", "cust_1", "web", "10.00", "2026-10-06 10:00:00")])
    row = df.first()
    assert row["dq_is_valid"] is True
    assert row["dq_failed_rules"] == []


def test_each_bad_value_is_flagged_by_its_rule(spark):
    df = flagged_rows(spark, [
        ("2", None, "web", "10", "2026-10-06 10:00:00"),
        ("3", "cust_3", "web", "-5", "2026-10-06 10:00:00"),
        ("4", "cust_4", "web", "250000", "2026-10-06 10:00:00"),
        ("5", "cust_5", "fax", "10", "2026-10-06 10:00:00"),
        ("6", "customer #6", "web", "10", "2026-10-06 10:00:00"),
        ("7", "cust_7", "web", "10", "31/02/2026 25:61"),
        ("8", "cust_8", "web", "10", "2099-01-01 00:00:00"),
        ("9", "cust_9", "web", "abc", "2026-10-06 10:00:00"),
    ])
    assert failed_for(df, 2) == {"customer_not_null", "customer_format"}
    assert failed_for(df, 3) == {"amount_in_range"}
    assert failed_for(df, 4) == {"amount_in_range"}
    assert failed_for(df, 5) == {"channel_accepted"}
    assert failed_for(df, 6) == {"customer_format"}
    assert failed_for(df, 7) == {"order_ts_parsed", "order_ts_not_in_future"}
    assert failed_for(df, 8) == {"order_ts_not_in_future"}
    assert failed_for(df, 9) == {"amount_in_range"}


def test_null_order_id_is_flagged(spark):
    df = flagged_rows(spark, [(None, "cust_1", "web", "10", "2026-10-06 10:00:00")])
    assert "order_id_not_null" in df.first()["dq_failed_rules"]


def test_duplicate_keeps_first_clean_record(spark):
    df = spark.createDataFrame(
        [
            ("1", "cust_1", "web", "10", "2026-10-06 10:00:00", datetime(2026, 10, 6, 11, 0)),
            ("1", "cust_1", "web", "10", "2026-10-06 10:00:00", datetime(2026, 10, 6, 12, 0)),
            ("1", "cust_1", "web", "-1", "2026-10-06 10:00:00", datetime(2026, 10, 6, 9, 0)),
        ],
        "order_id string, customer string, channel string, amount string, ts string, _ingested_at timestamp",
    )
    rows = r.flag_violations(t.parse_orders(df)).collect()
    valid = [x for x in rows if x["dq_is_valid"]]
    assert len(valid) == 1
    assert valid[0]["_ingested_at"] == datetime(2026, 10, 6, 11, 0)
    reasons = Counter(tuple(sorted(x["dq_failed_rules"])) for x in rows if not x["dq_is_valid"])
    assert reasons == {(r.DUPLICATE_RULE,): 1, ("amount_in_range",): 1}


def test_expectations_match_rules():
    exp = r.as_expectations(r.SILVER_RULES)
    assert set(exp) == {rule.name for rule in r.SILVER_RULES}
    assert all(" IS NOT NULL" in e for e in exp.values())


def test_generated_data_splits_into_expected_silver_and_quarantine(spark):
    rows = generator.generate_orders(n_valid=200, bad_per_kind=2, end=datetime(2026, 10, 6, 12, 0, 0))
    df = flagged_rows(spark, [(x["order_id"], x["customer"], x["channel"], x["amount"], x["ts"]) for x in rows])
    assert df.count() == 200 + 2 * len(generator.BAD_RECORD_KINDS)
    assert df.where("dq_is_valid").count() == 200
    assert df.where("NOT dq_is_valid").count() == 2 * len(generator.BAD_RECORD_KINDS)


def test_pipeline_selection_reconciles_bronze(spark):
    rows = generator.generate_orders(n_valid=300, bad_per_kind=2, end=datetime(2026, 10, 6, 12, 0, 0))
    flagged = flagged_rows(spark, [(x["order_id"], x["customer"], x["channel"], x["amount"], x["ts"]) for x in rows])
    silver = r.silver_candidates(flagged)
    for rule in r.SILVER_RULES:
        silver = silver.where(rule.expr)
    assert silver.count() == 300
    assert silver.select("order_id").distinct().count() == 300
    assert silver.count() + r.quarantined(flagged).count() == len(rows)
