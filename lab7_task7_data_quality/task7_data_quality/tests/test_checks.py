from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from orders_dq.quality import checks as c
from orders_dq.quality.reconciliation import reconcile_counts, reconcile_sums


@pytest.fixture
def people(spark):
    return spark.createDataFrame(
        [(1, "a", "web", Decimal("10.00")), (2, None, "web", Decimal("-1.00")), (2, "c", "fax", Decimal("5.00")),
         (4, "d", None, None)],
        "id int, name string, channel string, amount decimal(10,2)",
    )


def test_not_null_counts_per_column(people):
    res = {r.check_name: r for r in c.check_not_null(people, "silver", "t", ["id", "name", "amount"])}
    assert res["id_not_null"].status == c.PASS
    assert res["name_not_null"].failed_rows == 1
    assert res["name_not_null"].status == c.FAIL
    assert res["amount_not_null"].total_rows == 4


def test_not_null_threshold_allows_small_share(people):
    res = c.check_not_null(people, "silver", "t", ["name"], threshold=0.3)[0]
    assert res.status == c.PASS


def test_unique_counts_extra_rows(people):
    res = c.check_unique(people, "silver", "t", ["id"])
    assert (res.failed_rows, res.status) == (1, c.FAIL)


def test_accepted_values_treats_null_as_invalid(people):
    res = c.check_accepted_values(people, "silver", "t", "channel", ["web", "mobile"])
    assert res.failed_rows == 2


def test_range_check(people):
    res = c.check_range(people, "silver", "t", "amount", 0.01, 100)
    assert res.failed_rows == 2


def test_pattern_check(spark):
    df = spark.createDataFrame([("cust_1",), ("customer #2",), (None,)], "customer string")
    assert c.check_pattern(df, "silver", "t", "customer", "^cust_[0-9]+$").failed_rows == 2


def test_expression_check_counts_null_as_failure(spark):
    df = spark.createDataFrame([(1, 1), (1, 2), (None, 1)], "a int, b int")
    assert c.check_expression(df, "gold", "t", "a_eq_b", "a = b").failed_rows == 2


def test_referential_integrity_finds_orphans(spark):
    fact = spark.createDataFrame([(1,), (2,), (3,), (None,)], "customer_key int")
    dim = spark.createDataFrame([(1,), (2,)], "customer_key int")
    res = c.check_referential_integrity(fact, dim, "gold", "fact", "customer_key", "customer_key", "dim")
    assert (res.failed_rows, res.status) == (2, c.FAIL)
    assert res.dimension == "consistency"


def test_freshness_warns_but_does_not_block(spark):
    now = datetime(2026, 10, 7, 12, 0, 0)
    df = spark.createDataFrame([(now - timedelta(hours=72),)], "ts timestamp")
    res = c.check_freshness(df, "silver", "t", "ts", timedelta(hours=48), now=now)
    assert res.status == c.WARN
    fresh = c.check_freshness(df, "silver", "t", "ts", timedelta(hours=96), now=now)
    assert fresh.status == c.PASS


def test_freshness_on_empty_table_fails(spark):
    df = spark.createDataFrame([], "ts timestamp")
    res = c.check_freshness(df, "silver", "t", "ts", timedelta(hours=1), blocking=True)
    assert res.status == c.FAIL


def test_ratio_check():
    assert c.check_ratio(3, 100, "silver", "q", "quarantine_ratio", 0.05).status == c.PASS
    assert c.check_ratio(30, 100, "silver", "q", "quarantine_ratio", 0.05).status == c.FAIL


def test_reconcile_counts():
    assert reconcile_counts("x", "a", 10, "b", 10).status == c.PASS
    res = reconcile_counts("x", "a", 10, "b", 7)
    assert (res.failed_rows, res.status) == (3, c.FAIL)


def test_reconcile_sums_with_tolerance(spark):
    a = spark.createDataFrame([(Decimal("10.00"),), (Decimal("5.50"),)], "v decimal(10,2)")
    b = spark.createDataFrame([(Decimal("15.50"),)], "v decimal(18,2)")
    d = spark.createDataFrame([(Decimal("15.00"),)], "v decimal(18,2)")
    assert reconcile_sums("s", "a", a, "v", "b", b, "v").status == c.PASS
    assert reconcile_sums("s", "a", a, "v", "d", d, "v").status == c.FAIL
