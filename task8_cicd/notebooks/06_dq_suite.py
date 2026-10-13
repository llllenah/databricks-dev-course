# Databricks notebook source

import os
import sys
import uuid

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("freshness_hours", "48")
dbutils.widgets.text("max_quarantine_ratio", "0.05")
dbutils.widgets.text("src_path", "")

sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))
from orders_dq.quality.suite import MedallionTables, gate, results_to_df, run_suite, scorecard, summarize

catalog = dbutils.widgets.get("catalog")
bronze = f"{catalog}.{dbutils.widgets.get('bronze_schema')}"
silver = f"{catalog}.{dbutils.widgets.get('silver_schema')}"
gold = f"{catalog}.{dbutils.widgets.get('gold_schema')}"
run_id = f"suite-{uuid.uuid4().hex[:8]}"

tables = MedallionTables(
    bronze=spark.table(f"{bronze}.orders_bronze"),
    silver=spark.table(f"{silver}.orders_silver"),
    quarantine=spark.table(f"{silver}.orders_quarantine"),
    dim_customer=spark.table(f"{gold}.dim_customer"),
    dim_date=spark.table(f"{gold}.dim_date"),
    dim_time=spark.table(f"{gold}.dim_time"),
    fact=spark.table(f"{gold}.fact_orders"),
    agg_daily=spark.table(f"{gold}.agg_daily_sales"),
)

# COMMAND ----------

results = run_suite(
    tables,
    freshness_hours=int(dbutils.widgets.get("freshness_hours")),
    max_quarantine_ratio=float(dbutils.widgets.get("max_quarantine_ratio")),
)
results_df = results_to_df(spark, results, run_id=run_id)
results_df.write.mode("append").saveAsTable(f"{gold}.dq_results")

print(run_id, summarize(results))
display(scorecard(results_df))
display(results_df.orderBy("status", "layer", "check_name"))

# COMMAND ----------

gate(results)
