# Databricks notebook source
# DEV/TEST notebook (not part of the job): shows the SCD2 history of the cities dimension.

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("silver_schema", "lena066636_silver")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")

display(spark.sql(f"""
    SELECT city_id, city, population, effective_date, __START_AT, __END_AT,
           __END_AT IS NULL AS is_current
    FROM {catalog}.{silver_schema}.cities_scd2
    ORDER BY city_id, __START_AT
"""))
