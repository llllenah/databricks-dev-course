from pyspark.sql import functions as F

catalog = "dbr_dev_ua5816bd"
login = "lena066636"

# COMMAND ----------

invoice = (spark.table(f"{catalog}.{login}.invoice")
    .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate")))

invoice.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{login}.invoice")

invoice.printSchema()

# COMMAND ----------

track = (spark.table(f"{catalog}.{login}.track")
    .withColumn("Composer", F.coalesce(F.col("Composer"), F.lit("Unknown")))
    .withColumn("DurationMinutes", F.round(F.col("Milliseconds") / 60000, 2)))

track.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{login}.track")

track.select("Name", "Composer", "DurationMinutes").show(5)

# COMMAND ----------

spark.table(f"{catalog}.{login}.customer").filter(F.col("State").isNull()).count()