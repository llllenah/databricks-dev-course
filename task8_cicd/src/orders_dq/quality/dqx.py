"""Glue between Databricks Labs DQX and the results table of the custom suite.

DQX runs the checks from the YAML files in `checks/` and adds the `_errors` and `_warnings` columns to
every row. `check_results` turns those columns into one `CheckResult` per check, so DQX results land
in `gold.dq_results` next to the suite and appear on the same scorecard and dashboard.
"""

from pathlib import Path

import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .checks import CheckResult, make_result

ERRORS_COLUMN = "_errors"
WARNINGS_COLUMN = "_warnings"


def load_checks(path: str | Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _result_column(check: dict) -> str:
    return ERRORS_COLUMN if check.get("criticality", "error") == "error" else WARNINGS_COLUMN


def _failed(flagged: DataFrame, column: str, name: str) -> Column:
    # A check with for_each_column produces one item per column with the same name, so rows are counted
    # once per check, not once per item.
    if column not in flagged.columns:
        return F.lit(0)
    hit = F.exists(F.col(column), lambda item: item["name"] == F.lit(name))
    return F.sum(F.when(hit, 1).otherwise(0))


def check_results(flagged: DataFrame, checks: list[dict], layer: str, table: str) -> list[CheckResult]:
    """One result per DQX check: rows that failed the check out of all rows of the checked DataFrame."""
    row = flagged.agg(
        F.count(F.lit(1)).alias("_total"),
        *[_failed(flagged, _result_column(c), c["name"]).alias(f"_c{i}") for i, c in enumerate(checks)],
    ).first()
    total = row["_total"]
    return [
        make_result(
            layer,
            table,
            (c.get("user_metadata") or {}).get("dimension", "validity"),
            f"dqx_{c['name']}",
            row[f"_c{i}"] or 0,
            total,
            0.0,
            c.get("criticality", "error") == "error",
            details=c["check"]["function"],
        )
        for i, c in enumerate(checks)
    ]
