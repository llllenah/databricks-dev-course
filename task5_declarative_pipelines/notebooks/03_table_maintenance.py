# Databricks notebook source
# Maintenance task of the job: OPTIMIZE + VACUUM, run after both pipelines have finished.

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("bronze_schema", "lena066636_bronze")
dbutils.widgets.text("silver_schema", "lena066636_silver")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

tables = [
    f"{catalog}.{bronze_schema}.cities_landing",
    f"{catalog}.{bronze_schema}.ticks_stream",
    f"{catalog}.{silver_schema}.cities_clean",
    f"{catalog}.{silver_schema}.cities_scd2",
]

for table in tables:
    spark.sql(f"OPTIMIZE {table}")
    spark.sql(f"VACUUM {table}")
