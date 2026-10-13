# Databricks notebook source

import os
import sys

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("src_path", "")

sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))
from orders_dq.gold_ddl import overwrite_table
from orders_dq.transformations import (
    build_agg_channel_sales,
    build_agg_daily_sales,
    build_dim_customer,
    build_fact_orders,
)

catalog = dbutils.widgets.get("catalog")
silver = f"{catalog}.{dbutils.widgets.get('silver_schema')}"
gold = f"{catalog}.{dbutils.widgets.get('gold_schema')}"

orders = spark.table(f"{silver}.orders_silver")

# COMMAND ----------

overwrite_table(spark, build_dim_customer(orders), f"{gold}.dim_customer")
overwrite_table(spark, build_fact_orders(orders, spark.table(f"{gold}.dim_customer")), f"{gold}.fact_orders")

fact = spark.table(f"{gold}.fact_orders")
overwrite_table(spark, build_agg_daily_sales(fact), f"{gold}.agg_daily_sales")
overwrite_table(spark, build_agg_channel_sales(fact), f"{gold}.agg_channel_sales")

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'dim_customer' AS t, count(*) AS rows FROM {gold}.dim_customer
    UNION ALL SELECT 'fact_orders', count(*) FROM {gold}.fact_orders
    UNION ALL SELECT 'agg_daily_sales', count(*) FROM {gold}.agg_daily_sales
    UNION ALL SELECT 'agg_channel_sales', count(*) FROM {gold}.agg_channel_sales
"""))
