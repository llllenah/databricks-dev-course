from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .checks import CheckResult, make_result


def reconcile_counts(name: str, left_label: str, left_count: int, right_label: str, right_count: int,
                     blocking: bool = True) -> CheckResult:
    diff = abs(int(left_count) - int(right_count))
    return make_result("reconciliation", f"{left_label} vs {right_label}", "reconciliation", name, diff,
                   max(int(left_count), int(right_count)), 0.0, blocking,
                   details=f"{left_label}={left_count}, {right_label}={right_count}")


def reconcile_sums(name: str, left_label: str, left_df: DataFrame, left_col: str, right_label: str,
                   right_df: DataFrame, right_col: str, tolerance: float = 0.01,
                   blocking: bool = True) -> CheckResult:
    left = left_df.agg(F.sum(left_col)).first()[0] or Decimal(0)
    right = right_df.agg(F.sum(right_col)).first()[0] or Decimal(0)
    diff = abs(Decimal(str(left)) - Decimal(str(right)))
    failed = 0 if diff <= Decimal(str(tolerance)) else 1
    return make_result("reconciliation", f"{left_label} vs {right_label}", "reconciliation", name, failed, 1, 0.0,
                   blocking, details=f"{left_label}={left}, {right_label}={right}, diff={diff}")


def reconcile_layers(bronze: DataFrame, silver: DataFrame, quarantine: DataFrame, fact: DataFrame,
                     agg_daily: DataFrame) -> list[CheckResult]:
    bronze_rows = bronze.count()
    silver_rows = silver.count()
    quarantine_rows = quarantine.count()
    fact_rows = fact.count()
    agg_orders = agg_daily.agg(F.coalesce(F.sum("orders"), F.lit(0))).first()[0]
    return [
        reconcile_counts("bronze_eq_silver_plus_quarantine", "bronze", bronze_rows, "silver+quarantine",
                         silver_rows + quarantine_rows),
        reconcile_counts("silver_eq_fact", "silver", silver_rows, "fact_orders", fact_rows),
        reconcile_counts("fact_eq_agg_daily_orders", "fact_orders", fact_rows, "agg_daily_sales.orders",
                         agg_orders),
        reconcile_sums("silver_amount_eq_fact_amount", "silver.amount", silver, "amount", "fact_orders.amount",
                       fact, "amount"),
        reconcile_sums("fact_amount_eq_agg_daily_revenue", "fact_orders.amount", fact, "amount",
                       "agg_daily_sales.revenue", agg_daily, "revenue"),
    ]
