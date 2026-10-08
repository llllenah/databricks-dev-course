# Databricks notebook source

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("silver_schema", "lena066636_dq_silver")

quarantine = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('silver_schema')}.orders_quarantine"

# COMMAND ----------

display(spark.sql(f"""
    SELECT rule, count(*) AS rows
    FROM {quarantine} LATERAL VIEW explode(dq_failed_rules) r AS rule
    GROUP BY rule ORDER BY rows DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT order_id, customer, channel, amount, order_ts, dq_failed_rules, _source_file
    FROM {quarantine}
    ORDER BY dq_failed_rules[0], order_id
"""))
