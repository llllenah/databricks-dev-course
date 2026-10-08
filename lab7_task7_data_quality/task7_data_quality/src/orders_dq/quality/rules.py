from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

ACCEPTED_CHANNELS = ("web", "mobile", "store")
ACCEPTED_SEGMENTS = ("premium", "standard")
MIN_AMOUNT = 0.01
MAX_AMOUNT = 10000
CUSTOMER_PATTERN = "^cust_[0-9]+$"
DUPLICATE_RULE = "unique_order_id"


@dataclass(frozen=True)
class Rule:
    name: str
    expr: str
    dimension: str


BRONZE_FAIL_RULES = [
    Rule("source_file_present", "_source_file IS NOT NULL", "completeness"),
]

SILVER_RULES = [
    Rule("order_id_not_null", "order_id IS NOT NULL", "completeness"),
    Rule("customer_not_null", "customer IS NOT NULL", "completeness"),
    Rule("order_ts_parsed", "order_ts IS NOT NULL", "validity"),
    Rule(
        "amount_in_range",
        f"amount IS NOT NULL AND amount BETWEEN {MIN_AMOUNT} AND {MAX_AMOUNT}",
        "validity",
    ),
    Rule(
        "channel_accepted",
        "channel IS NOT NULL AND channel IN (" + ", ".join(f"'{c}'" for c in ACCEPTED_CHANNELS) + ")",
        "validity",
    ),
    Rule(
        "customer_format",
        f"customer IS NOT NULL AND customer RLIKE '{CUSTOMER_PATTERN}'",
        "validity",
    ),
    Rule(
        "order_ts_not_in_future",
        "order_ts IS NOT NULL AND order_ts <= current_timestamp() + INTERVAL 1 HOUR",
        "timeliness",
    ),
]


def as_expectations(rules: list[Rule]) -> dict[str, str]:
    return {r.name: r.expr for r in rules}


def failed_rules(rules: list[Rule]) -> Column:
    checks = [F.when(~F.coalesce(F.expr(r.expr), F.lit(False)), F.lit(r.name)) for r in rules]
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def flag_violations(orders: DataFrame, rules: list[Rule] = SILVER_RULES) -> DataFrame:
    order_cols = [c for c in ("_ingested_at", "_source_file") if c in orders.columns] + ["order_ts"]
    flagged = orders.withColumn("dq_failed_rules", failed_rules(rules))
    clean = F.size("dq_failed_rules") == 0
    rank = F.when(
        clean,
        F.row_number().over(
            Window.partitionBy("order_id", clean).orderBy(*[F.col(c).asc_nulls_last() for c in order_cols])
        ),
    )
    return (
        flagged.withColumn("_dup_rank", rank)
        .withColumn(
            "dq_failed_rules",
            F.when(F.col("_dup_rank") > 1, F.array_union("dq_failed_rules", F.array(F.lit(DUPLICATE_RULE)))).otherwise(
                F.col("dq_failed_rules")
            ),
        )
        .withColumn("dq_is_valid", F.size("dq_failed_rules") == 0)
    )


def silver_candidates(flagged: DataFrame) -> DataFrame:
    return flagged.where(F.col("_dup_rank").isNull() | (F.col("_dup_rank") == 1))


def quarantined(flagged: DataFrame) -> DataFrame:
    return flagged.where(~F.col("dq_is_valid"))
