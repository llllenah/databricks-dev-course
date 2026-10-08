from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"


@dataclass(frozen=True)
class CheckResult:
    layer: str
    table_name: str
    dimension: str
    check_name: str
    failed_rows: int
    total_rows: int
    threshold: float
    blocking: bool
    status: str
    details: str = ""

    @property
    def failed_ratio(self) -> float:
        return self.failed_rows / self.total_rows if self.total_rows else 0.0

    def as_row(self) -> dict:
        row = asdict(self)
        row["failed_ratio"] = round(self.failed_ratio, 6)
        return row


def _status(failed: int, total: int, threshold: float, blocking: bool) -> str:
    ratio = failed / total if total else 0.0
    if failed == 0 or ratio <= threshold:
        return PASS
    return FAIL if blocking else WARN


def make_result(layer, table, dimension, name, failed, total, threshold, blocking, details="") -> CheckResult:
    return CheckResult(
        layer=layer,
        table_name=table,
        dimension=dimension,
        check_name=name,
        failed_rows=int(failed),
        total_rows=int(total),
        threshold=float(threshold),
        blocking=blocking,
        status=_status(int(failed), int(total), threshold, blocking),
        details=details,
    )


def check_not_null(df: DataFrame, layer: str, table: str, columns: Iterable[str],
                   threshold: float = 0.0, blocking: bool = True) -> list[CheckResult]:
    columns = list(columns)
    row = df.agg(
        F.count(F.lit(1)).alias("_total"),
        *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns],
    ).first()
    total = row["_total"]
    return [
        make_result(layer, table, "completeness", f"{c}_not_null", row[c] or 0, total, threshold, blocking)
        for c in columns
    ]


def check_unique(df: DataFrame, layer: str, table: str, keys: Iterable[str],
                 blocking: bool = True) -> CheckResult:
    keys = list(keys)
    total = df.count()
    duplicates = df.groupBy(*keys).count().where("count > 1").agg(
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("extra")
    ).first()["extra"]
    return make_result(layer, table, "uniqueness", f"unique_{'_'.join(keys)}", duplicates, total, 0.0, blocking)


def check_accepted_values(df: DataFrame, layer: str, table: str, column: str, accepted: Iterable[str],
                          blocking: bool = True) -> CheckResult:
    accepted = list(accepted)
    row = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col(column).isNull() | ~F.col(column).isin(accepted), 1).otherwise(0)).alias("bad"),
    ).first()
    return make_result(layer, table, "validity", f"{column}_accepted_values", row["bad"] or 0, row["total"],
                   0.0, blocking, details=",".join(accepted))


def check_range(df: DataFrame, layer: str, table: str, column: str, min_value=None, max_value=None,
                blocking: bool = True) -> CheckResult:
    cond = F.col(column).isNull()
    if min_value is not None:
        cond = cond | (F.col(column) < min_value)
    if max_value is not None:
        cond = cond | (F.col(column) > max_value)
    row = df.agg(F.count(F.lit(1)).alias("total"), F.sum(F.when(cond, 1).otherwise(0)).alias("bad")).first()
    return make_result(layer, table, "validity", f"{column}_in_range", row["bad"] or 0, row["total"], 0.0,
                   blocking, details=f"[{min_value}, {max_value}]")


def check_pattern(df: DataFrame, layer: str, table: str, column: str, pattern: str,
                  blocking: bool = True) -> CheckResult:
    row = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col(column).isNull() | ~F.col(column).rlike(pattern), 1).otherwise(0)).alias("bad"),
    ).first()
    return make_result(layer, table, "validity", f"{column}_format", row["bad"] or 0, row["total"], 0.0,
                   blocking, details=pattern)


def check_expression(df: DataFrame, layer: str, table: str, name: str, expr: str, dimension: str = "consistency",
                     blocking: bool = True) -> CheckResult:
    row = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(~F.coalesce(F.expr(expr), F.lit(False)), 1).otherwise(0)).alias("bad"),
    ).first()
    return make_result(layer, table, dimension, name, row["bad"] or 0, row["total"], 0.0, blocking, details=expr)


def check_referential_integrity(child: DataFrame, parent: DataFrame, layer: str, table: str, child_key: str,
                                parent_key: str, parent_name: str, blocking: bool = True) -> CheckResult:
    total = child.count()
    orphans = child.join(
        parent.select(F.col(parent_key).alias("_pk")).distinct(),
        F.col(child_key) == F.col("_pk"),
        "left_anti",
    ).count()
    return make_result(layer, table, "consistency", f"{child_key}_exists_in_{parent_name}", orphans, total, 0.0,
                   blocking)


def check_freshness(df: DataFrame, layer: str, table: str, ts_column: str, max_age: timedelta,
                    now: Optional[datetime] = None, blocking: bool = False) -> CheckResult:
    latest = df.agg(F.max(ts_column).alias("latest")).first()["latest"]
    now = now or datetime.now()
    if latest is None:
        return make_result(layer, table, "timeliness", f"{ts_column}_fresh", 1, 1, 0.0, blocking, "no rows")
    age = now - latest
    failed = 1 if age > max_age else 0
    return make_result(layer, table, "timeliness", f"{ts_column}_fresh", failed, 1, 0.0, blocking,
                   details=f"latest={latest.isoformat(sep=' ')}, age_hours={age.total_seconds() / 3600:.1f}")


def check_ratio(failed: int, total: int, layer: str, table: str, name: str, threshold: float,
                dimension: str = "validity", blocking: bool = True) -> CheckResult:
    return make_result(layer, table, dimension, name, failed, total, threshold, blocking,
                   details=f"max_ratio={threshold}")
