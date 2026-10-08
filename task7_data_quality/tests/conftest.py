import os
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _active_session():
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        from databricks.sdk.runtime import spark as runtime_spark

        return runtime_spark
    try:
        from pyspark.sql import SparkSession

        return SparkSession.getActiveSession()
    except Exception:
        return None


def _connect_session():
    from databricks.connect import DatabricksSession

    builder = DatabricksSession.builder
    if os.getenv("DATABRICKS_CLUSTER_ID"):
        return builder.getOrCreate()
    return builder.serverless(True).getOrCreate()


def _local_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("orders-dq-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def spark():
    mode = os.getenv("SPARK_MODE", "auto").lower()
    session = _active_session() if mode in ("auto", "databricks") else None
    if session is None and (mode == "connect" or (mode == "auto" and os.getenv("DATABRICKS_HOST"))):
        session = _connect_session()
    if session is None:
        session = _local_session()
    yield session
