# Databricks notebook source
# MAGIC %pip install -q databricks-labs-dqx==0.16.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
import uuid

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("src_path", "")
dbutils.widgets.text("checks_path", "")

sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from orders_dq.quality.dqx import check_results, load_checks
from orders_dq.quality.suite import gate, results_to_df, summarize

catalog = dbutils.widgets.get("catalog")
silver = f"{catalog}.{dbutils.widgets.get('silver_schema')}"
gold = f"{catalog}.{dbutils.widgets.get('gold_schema')}"
checks_path = dbutils.widgets.get("checks_path") or os.path.abspath("../checks")
run_id = f"dqx-{uuid.uuid4().hex[:8]}"

dq_engine = DQEngine(WorkspaceClient())

# (layer, table, data, reference DataFrames for foreign_key checks)
targets = [
    ("silver", "orders_silver", spark.table(f"{silver}.orders_silver"), {}),
    (
        "gold",
        "fact_orders",
        spark.table(f"{gold}.fact_orders"),
        {
            "dim_customer": spark.table(f"{gold}.dim_customer"),
            "dim_date": spark.table(f"{gold}.dim_date"),
            "dim_time": spark.table(f"{gold}.dim_time"),
        },
    ),
]

# COMMAND ----------

results = []
for layer, table, df, ref_dfs in targets:
    checks = load_checks(f"{checks_path}/{table}.yml")
    status = DQEngine.validate_checks(checks)
    if status.has_errors:
        raise ValueError(f"invalid DQX checks for {table}: {status}")

    flagged = dq_engine.apply_checks_by_metadata(df, checks, ref_dfs=ref_dfs)
    # Rows with errors or warnings and the reasons, for review. Overwritten on every run.
    dq_engine.get_invalid(flagged).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{gold}.dqx_{table}_issues"
    )
    results += check_results(flagged, checks, layer, table)

results_df = results_to_df(spark, results, run_id=run_id)
results_df.write.mode("append").saveAsTable(f"{gold}.dq_results")

print(run_id, summarize(results))
display(results_df.orderBy("status", "layer", "check_name"))

# COMMAND ----------

gate(results)
