from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import checks as c
from .reconciliation import reconcile_layers
from .rules import ACCEPTED_CHANNELS, ACCEPTED_SEGMENTS, CUSTOMER_PATTERN, MAX_AMOUNT, MIN_AMOUNT

RESULT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("run_ts", TimestampType()),
        StructField("layer", StringType()),
        StructField("table_name", StringType()),
        StructField("dimension", StringType()),
        StructField("check_name", StringType()),
        StructField("status", StringType()),
        StructField("blocking", BooleanType()),
        StructField("failed_rows", LongType()),
        StructField("total_rows", LongType()),
        StructField("failed_ratio", DoubleType()),
        StructField("threshold", DoubleType()),
        StructField("details", StringType()),
    ]
)


class DataQualityError(Exception):
    pass


@dataclass
class MedallionTables:
    bronze: DataFrame
    silver: DataFrame
    quarantine: DataFrame
    dim_customer: DataFrame
    dim_date: DataFrame
    dim_time: DataFrame
    fact: DataFrame
    agg_daily: DataFrame


def run_suite(t: MedallionTables, now: Optional[datetime] = None, freshness_hours: int = 48,
              max_quarantine_ratio: float = 0.05) -> list[c.CheckResult]:
    results: list[c.CheckResult] = []

    results += c.check_not_null(t.bronze, "bronze", "orders_bronze", ["_source_file", "_ingested_at"])
    results.append(c.check_freshness(t.bronze, "bronze", "orders_bronze", "_ingested_at",
                                     timedelta(hours=freshness_hours), now=now))

    results += c.check_not_null(t.silver, "silver", "orders_silver",
                                ["order_id", "customer", "channel", "amount", "order_ts"])
    results.append(c.check_unique(t.silver, "silver", "orders_silver", ["order_id"]))
    results.append(c.check_range(t.silver, "silver", "orders_silver", "amount", MIN_AMOUNT, MAX_AMOUNT))
    results.append(c.check_accepted_values(t.silver, "silver", "orders_silver", "channel", ACCEPTED_CHANNELS))
    results.append(c.check_pattern(t.silver, "silver", "orders_silver", "customer", CUSTOMER_PATTERN))
    results.append(c.check_freshness(t.silver, "silver", "orders_silver", "order_ts",
                                     timedelta(hours=freshness_hours), now=now))
    results.append(c.check_ratio(t.quarantine.count(), t.bronze.count(), "silver", "orders_quarantine",
                                 "quarantine_ratio", max_quarantine_ratio))

    results.append(c.check_unique(t.dim_customer, "gold", "dim_customer", ["customer_key"]))
    results.append(c.check_unique(t.dim_customer, "gold", "dim_customer", ["customer_name"]))
    results.append(c.check_accepted_values(t.dim_customer, "gold", "dim_customer", "segment", ACCEPTED_SEGMENTS))
    results.append(c.check_unique(t.dim_date, "gold", "dim_date", ["date_key"]))
    results.append(c.check_expression(t.dim_date, "gold", "dim_date", "date_key_matches_date",
                                      "date_key = cast(date_format(date, 'yyyyMMdd') AS INT)"))
    results.append(c.check_unique(t.dim_time, "gold", "dim_time", ["time_key"]))
    results.append(c.check_ratio(abs(t.dim_time.count() - 24), 24, "gold", "dim_time", "dim_time_has_24_hours",
                                 0.0, dimension="completeness"))

    results += c.check_not_null(t.fact, "gold", "fact_orders",
                                ["order_id", "date_key", "time_key", "customer_key", "amount"])
    results.append(c.check_unique(t.fact, "gold", "fact_orders", ["order_id"]))
    results.append(c.check_range(t.fact, "gold", "fact_orders", "time_key", 0, 23))
    results.append(c.check_range(t.fact, "gold", "fact_orders", "amount", MIN_AMOUNT, MAX_AMOUNT))
    results.append(c.check_referential_integrity(t.fact, t.dim_date, "gold", "fact_orders", "date_key", "date_key",
                                                 "dim_date"))
    results.append(c.check_referential_integrity(t.fact, t.dim_time, "gold", "fact_orders", "time_key", "time_key",
                                                 "dim_time"))
    results.append(c.check_referential_integrity(t.fact, t.dim_customer, "gold", "fact_orders", "customer_key",
                                                 "customer_key", "dim_customer"))
    results.append(c.check_expression(
        t.fact.alias("f").join(t.dim_customer.alias("d"), "customer_key"),
        "gold", "fact_orders", "segment_matches_dim_customer", "f.segment = d.segment",
    ))

    results += reconcile_layers(t.bronze, t.silver, t.quarantine, t.fact, t.agg_daily)
    return results


def results_to_df(spark: SparkSession, results: list[c.CheckResult], run_id: str,
                  run_ts: Optional[datetime] = None) -> DataFrame:
    run_ts = run_ts or datetime.now()
    rows = []
    for r in results:
        d = r.as_row()
        rows.append((run_id, run_ts, d["layer"], d["table_name"], d["dimension"], d["check_name"], d["status"],
                     d["blocking"], d["failed_rows"], d["total_rows"], d["failed_ratio"], d["threshold"],
                     d["details"]))
    return spark.createDataFrame(rows, RESULT_SCHEMA)


def summarize(results: list[c.CheckResult]) -> dict:
    return {
        "total": len(results),
        "passed": sum(r.status == c.PASS for r in results),
        "warnings": sum(r.status == c.WARN for r in results),
        "failed": sum(r.status == c.FAIL for r in results),
    }


def gate(results: list[c.CheckResult]) -> None:
    failures = [r for r in results if r.status == c.FAIL]
    if failures:
        lines = [f"{r.layer}.{r.table_name}: {r.check_name} ({r.failed_rows}/{r.total_rows}) {r.details}"
                 for r in failures]
        raise DataQualityError("Blocking data quality checks failed:\n" + "\n".join(lines))


def scorecard(results_df: DataFrame) -> DataFrame:
    return (
        results_df.groupBy("layer", "dimension")
        .agg(
            F.count("*").alias("checks"),
            F.sum(F.when(F.col("status") == c.PASS, 1).otherwise(0)).alias("passed"),
            F.sum(F.when(F.col("status") == c.WARN, 1).otherwise(0)).alias("warnings"),
            F.sum(F.when(F.col("status") == c.FAIL, 1).otherwise(0)).alias("failed"),
        )
        .withColumn("score_pct", F.round(F.col("passed") * 100.0 / F.col("checks"), 1))
        .orderBy("layer", "dimension")
    )
