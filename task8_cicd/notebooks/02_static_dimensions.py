# Databricks notebook source

import os
import sys
from datetime import date

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("start_year", "2024")
dbutils.widgets.text("end_year", "2030")
dbutils.widgets.dropdown("recreate", "no", ["no", "yes"])
dbutils.widgets.text("src_path", "")

sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))
from orders_dq.gold_ddl import overwrite_table
from orders_dq.transformations import build_dim_date, build_dim_time

gold = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('gold_schema')}"
recreate = dbutils.widgets.get("recreate") == "yes"

# COMMAND ----------

if recreate or spark.table(f"{gold}.dim_date").isEmpty():
    start = date(int(dbutils.widgets.get("start_year")), 1, 1)
    end = date(int(dbutils.widgets.get("end_year")), 12, 31)
    overwrite_table(spark, build_dim_date(spark, start, end), f"{gold}.dim_date")
    print("dim_date loaded")
else:
    print("dim_date already loaded, skipped")

if recreate or spark.table(f"{gold}.dim_time").count() != 24:
    overwrite_table(spark, build_dim_time(spark), f"{gold}.dim_time")
    print("dim_time loaded")
else:
    print("dim_time already loaded, skipped")

display(spark.sql(f"SELECT 'dim_date' AS t, count(*) AS rows FROM {gold}.dim_date UNION ALL SELECT 'dim_time', count(*) FROM {gold}.dim_time"))
