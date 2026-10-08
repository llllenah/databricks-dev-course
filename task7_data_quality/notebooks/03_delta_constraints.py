# Databricks notebook source

import os
import sys
import uuid

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("gold_schema", "lena066636_dq_gold")
dbutils.widgets.text("src_path", "")

sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))
from orders_dq.gold_ddl import CHECK_CONSTRAINTS
from orders_dq.quality.checks import make_result
from orders_dq.quality.suite import gate, results_to_df

gold = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('gold_schema')}"
fact = f"{gold}.fact_orders"
probe_id = 999999999

# COMMAND ----------
# MAGIC %md
# MAGIC ## Constraints on the gold tables

# COMMAND ----------

display(spark.sql(f"SHOW TBLPROPERTIES {fact}").where("key LIKE 'delta.constraints.%'"))
display(spark.sql(f"DESCRIBE TABLE {fact}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Invalid writes must be rejected

# COMMAND ----------

cases = {
    "rejects_negative_amount": f"({probe_id}, 20261006, 10, 1, 'standard', 'web', -5.00)",
    "rejects_hour_out_of_range": f"({probe_id}, 20261006, 25, 1, 'standard', 'web', 10.00)",
    "rejects_unknown_channel": f"({probe_id}, 20261006, 10, 1, 'standard', 'fax', 10.00)",
    "rejects_null_customer_key": f"({probe_id}, 20261006, 10, NULL, 'standard', 'web', 10.00)",
    "rejects_bad_date_key": f"({probe_id}, 1006, 10, 1, 'standard', 'web', 10.00)",
}

results = []
for name, values in cases.items():
    try:
        spark.sql(f"INSERT INTO {fact} VALUES {values}")
        spark.sql(f"DELETE FROM {fact} WHERE order_id = {probe_id}")
        accepted, message = 1, "invalid row was accepted"
    except Exception as e:
        accepted, message = 0, str(e).split("\n")[0][:300]
    results.append(make_result("gold", "fact_orders", "constraints", name, accepted, 1, 0.0, True, message))
    print(f"{name}: {'REJECTED' if not accepted else 'ACCEPTED'} | {message}")

# COMMAND ----------

results_df = results_to_df(spark, results, run_id=f"constraints-{uuid.uuid4().hex[:8]}")
results_df.write.mode("append").saveAsTable(f"{gold}.dq_results")
display(results_df)
gate(results)
