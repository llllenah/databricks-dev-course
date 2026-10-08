import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

sys.path.insert(0, spark.conf.get("src_path"))

from orders_dq.quality.rules import (
    BRONZE_FAIL_RULES,
    SILVER_RULES,
    as_expectations,
    flag_violations,
    quarantined,
    silver_candidates,
)
from orders_dq.transformations import RAW_ORDER_SCHEMA, parse_orders

CATALOG = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")
LANDING_PATH = spark.conf.get("landing_path")

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.orders_bronze"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.orders_silver"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.orders_quarantine"

SILVER_COLUMNS = ["order_id", "customer", "channel", "amount", "order_ts", "_source_file", "_ingested_at"]


@dp.table(
    name=BRONZE_TABLE,
    comment="Raw orders from JSON files in the landing volume. All columns are kept as strings.",
)
@dp.expect_all_or_fail(as_expectations(BRONZE_FAIL_RULES))
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(RAW_ORDER_SCHEMA)
        .load(LANDING_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.temporary_view(name="orders_validated")
def orders_validated():
    return flag_violations(parse_orders(spark.read.table(BRONZE_TABLE)), SILVER_RULES)


@dp.materialized_view(
    name=SILVER_TABLE,
    comment="Typed, validated and deduplicated orders. Invalid rows are dropped by expectations.",
)
@dp.expect_all_or_drop(as_expectations(SILVER_RULES))
def orders_silver():
    return (
        silver_candidates(spark.read.table("orders_validated"))
        .select(*SILVER_COLUMNS)
    )


@dp.materialized_view(
    name=QUARANTINE_TABLE,
    comment="Rejected orders with the list of failed rules. One row per rejected bronze record.",
)
def orders_quarantine():
    return (
        quarantined(spark.read.table("orders_validated"))
        .select(*SILVER_COLUMNS, "dq_failed_rules", F.current_timestamp().alias("quarantined_at"))
    )
