# Databricks notebook source

import json
import os
import sys

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("bronze_schema", "lena066636_dq_bronze")
dbutils.widgets.text("silver_schema", "lena066636_dq_silver")
dbutils.widgets.text("gold_schema", "lena066636_dq_gold")
dbutils.widgets.text("n_valid", "1000")
dbutils.widgets.text("bad_per_kind", "3")
dbutils.widgets.dropdown("reset", "yes", ["yes", "no"])
dbutils.widgets.text("src_path", "")

sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))
from orders_dq.generator import BAD_RECORD_KINDS, generate_orders, split_batches

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
n_valid = int(dbutils.widgets.get("n_valid"))
bad_per_kind = int(dbutils.widgets.get("bad_per_kind"))

for schema in (bronze_schema, dbutils.widgets.get("silver_schema"), dbutils.widgets.get("gold_schema")):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.landing")

landing_path = f"/Volumes/{catalog}/{bronze_schema}/landing/orders"

# COMMAND ----------

if dbutils.widgets.get("reset") == "yes":
    dbutils.fs.rm(landing_path, True)
dbutils.fs.mkdirs(landing_path)

rows = generate_orders(n_valid=n_valid, bad_per_kind=bad_per_kind)
for i, batch in enumerate(split_batches(rows), start=1):
    dbutils.fs.put(f"{landing_path}/orders_batch_{i}.json", "\n".join(json.dumps(r) for r in batch), True)

print(f"valid orders: {n_valid}, bad records: {bad_per_kind * len(BAD_RECORD_KINDS)}, total: {len(rows)}")
display(dbutils.fs.ls(landing_path))
