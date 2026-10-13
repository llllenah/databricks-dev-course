# Databricks notebook source

import os
import sys

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("src_path", "")

sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))
from orders_dq.gold_ddl import TABLES, ensure_table, scorecard_view_sql

gold = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('gold_schema')}"

# COMMAND ----------
# MAGIC %md
# MAGIC Gold tables, their constraints and the scorecard view are created from code.
# MAGIC Every statement is idempotent: `CREATE TABLE IF NOT EXISTS`, missing constraints only, `CREATE OR REPLACE VIEW`.

# COMMAND ----------

for table in TABLES:
    ensure_table(spark, gold, table)
    print(f"{gold}.{table} ready")

spark.sql(scorecard_view_sql(gold))
display(spark.sql(f"SHOW TABLES IN {gold}"))
