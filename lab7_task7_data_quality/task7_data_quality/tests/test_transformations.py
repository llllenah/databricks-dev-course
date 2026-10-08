from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from orders_dq import transformations as t


def raw(spark, rows):
    return spark.createDataFrame(rows, t.RAW_ORDER_SCHEMA)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-10-06 14:05:09", datetime(2026, 10, 6, 14, 5, 9)),
        ("2026-10-06T14:05:09", datetime(2026, 10, 6, 14, 5, 9)),
        (" 06.10.2026 14:05:09 ", datetime(2026, 10, 6, 14, 5, 9)),
        ("31/02/2026 25:61", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_order_ts(spark, value, expected):
    df = spark.createDataFrame([(value,)], "ts string")
    assert df.select(t.parse_order_ts(F.col("ts")).alias("v")).first()["v"] == expected


def test_parse_orders_casts_and_normalizes(spark):
    df = t.parse_orders(raw(spark, [(" 7 ", " Cust_3 ", " WEB ", " 19.90 ", "2026-10-06 10:00:00")]))
    row = df.first()
    assert row["order_id"] == 7
    assert row["customer"] == "cust_3"
    assert row["channel"] == "web"
    assert row["amount"] == Decimal("19.90")
    assert row["order_ts"] == datetime(2026, 10, 6, 10, 0, 0)


def test_parse_orders_turns_bad_values_into_nulls(spark):
    df = t.parse_orders(raw(spark, [("abc", None, None, "12,5O", "not a date")]))
    row = df.first()
    assert row["order_id"] is None
    assert row["amount"] is None
    assert row["order_ts"] is None


def test_parse_orders_keeps_ingestion_metadata(spark):
    df = raw(spark, [("1", "cust_1", "web", "10", "2026-10-06 10:00:00")]).withColumn(
        "_source_file", F.lit("batch_1.json")
    )
    assert "_source_file" in t.parse_orders(df).columns


@pytest.mark.parametrize(
    "hour, part",
    [(0, "night"), (5, "night"), (6, "morning"), (11, "morning"), (12, "afternoon"), (17, "afternoon"),
     (18, "evening"), (23, "evening")],
)
def test_day_part_boundaries(spark, hour, part):
    df = spark.createDataFrame([(hour,)], "h int")
    assert df.select(t.day_part(F.col("h")).alias("p")).first()["p"] == part


@pytest.mark.parametrize(
    "amount, bucket",
    [(Decimal("49.99"), "1) under 50"), (Decimal("50"), "2) 50-100"), (Decimal("149.99"), "3) 100-150"),
     (Decimal("150"), "4) 150+")],
)
def test_amount_bucket(spark, amount, bucket):
    df = spark.createDataFrame([(amount,)], "a decimal(10,2)")
    assert df.select(t.amount_bucket(F.col("a")).alias("b")).first()["b"] == bucket


def test_date_and_time_keys(spark):
    df = spark.createDataFrame([(datetime(2026, 1, 9, 23, 59, 59),)], "ts timestamp")
    row = df.select(t.date_key(F.col("ts")).alias("d"), t.time_key(F.col("ts")).alias("h")).first()
    assert (row["d"], row["h"]) == (20260109, 23)


def test_dim_time_has_24_unique_hours(spark):
    dim = t.build_dim_time(spark)
    assert dim.count() == 24
    assert dim.select("time_key").distinct().count() == 24
    assert dim.where("time_key = 9").first()["hour_label"] == "09:00-09:59"
    assert dim.where("is_business_hours").count() == 9


def test_dim_date_covers_range_once(spark):
    dim = t.build_dim_date(spark, date(2024, 1, 1), date(2024, 12, 31))
    assert dim.count() == 366
    assert dim.select("date_key").distinct().count() == 366
    first = dim.orderBy("date_key").first()
    assert (first["date_key"], first["year"], first["day_name"]) == (20240101, 2024, "Monday")


@pytest.fixture
def orders(spark):
    rows = [(i, f"cust_{i % 10}", "web", Decimal(10 * (i % 10 + 1)), datetime(2026, 10, 6, i % 24, 0, 0))
            for i in range(1, 51)]
    return spark.createDataFrame(rows, "order_id int, customer string, channel string, amount decimal(10,2), "
                                       "order_ts timestamp")


def test_dim_customer_premium_is_top_fifth_by_spend(spark, orders):
    dim = t.build_dim_customer(orders)
    assert dim.count() == 10
    premium = {r["customer_name"] for r in dim.where("segment = 'premium'").collect()}
    assert premium == {"cust_9", "cust_8"}


def test_fact_has_keys_and_no_raw_timestamp(spark, orders):
    fact = t.build_fact_orders(orders, t.build_dim_customer(orders))
    assert fact.count() == orders.count()
    assert "order_ts" not in fact.columns
    assert fact.where("customer_key IS NULL OR date_key IS NULL OR time_key IS NULL").count() == 0
    assert set(fact.columns) == {"order_id", "date_key", "time_key", "customer_key", "segment", "channel", "amount"}


def test_agg_daily_sales_matches_fact(spark, orders):
    fact = t.build_fact_orders(orders, t.build_dim_customer(orders))
    agg = t.build_agg_daily_sales(fact).first()
    total = fact.agg(F.sum("amount")).first()[0]
    assert agg["orders"] == 50
    assert agg["revenue"] == total
