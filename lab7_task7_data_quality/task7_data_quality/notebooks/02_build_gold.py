# Databricks notebook source

import os
import sys

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("silver_schema", "lena066636_dq_silver")
dbutils.widgets.text("gold_schema", "lena066636_dq_gold")
dbutils.widgets.text("viewer_group", "account users")
dbutils.widgets.text("src_path", "")

sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))
from orders_dq.gold_ddl import ensure_table, overwrite_table
from orders_dq.transformations import (
    build_agg_channel_sales,
    build_agg_daily_sales,
    build_dim_customer,
    build_fact_orders,
)

catalog = dbutils.widgets.get("catalog")
silver = f"{catalog}.{dbutils.widgets.get('silver_schema')}"
gold = f"{catalog}.{dbutils.widgets.get('gold_schema')}"

for table in ("dim_customer", "fact_orders", "agg_daily_sales", "agg_channel_sales"):
    ensure_table(spark, gold, table)

orders = spark.table(f"{silver}.orders_silver")

# COMMAND ----------

overwrite_table(spark, build_dim_customer(orders), f"{gold}.dim_customer")
overwrite_table(spark, build_fact_orders(orders, spark.table(f"{gold}.dim_customer")), f"{gold}.fact_orders")

fact = spark.table(f"{gold}.fact_orders")
overwrite_table(spark, build_agg_daily_sales(fact), f"{gold}.agg_daily_sales")
overwrite_table(spark, build_agg_channel_sales(fact), f"{gold}.agg_channel_sales")

# COMMAND ----------

viewer = dbutils.widgets.get("viewer_group")
if viewer:
    for schema in (silver, gold):
        try:
            spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA {schema} TO `{viewer}`")
        except Exception as e:
            print(f"grant on {schema} skipped: {e}")

display(spark.sql(f"""
    SELECT 'dim_customer' AS t, count(*) AS rows FROM {gold}.dim_customer
    UNION ALL SELECT 'fact_orders', count(*) FROM {gold}.fact_orders
    UNION ALL SELECT 'agg_daily_sales', count(*) FROM {gold}.agg_daily_sales
    UNION ALL SELECT 'agg_channel_sales', count(*) FROM {gold}.agg_channel_sales
"""))
