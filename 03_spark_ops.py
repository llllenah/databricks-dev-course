from pyspark.sql import functions as F

catalog = "dbr_dev_ua5816bd"
login = "lena066636"

track = spark.table(f"{catalog}.{login}.track")
invoice_line = spark.table(f"{catalog}.{login}.invoiceline")
invoice = spark.table(f"{catalog}.{login}.invoice")
customer = spark.table(f"{catalog}.{login}.customer")
genre = spark.table(f"{catalog}.{login}.genre")
album = spark.table(f"{catalog}.{login}.album")
artist = spark.table(f"{catalog}.{login}.artist")

# COMMAND ----------

track.select("Name", "Composer", "DurationMinutes", "UnitPrice").show(5)
track.filter(F.col("DurationMinutes") > 5).select("Name", "DurationMinutes").show(5)

# COMMAND ----------

revenue_by_genre = (invoice_line
    .join(track, "TrackId")
    .join(genre, "GenreId")
    .select(genre["Name"].alias("GenreName"), invoice_line["UnitPrice"], "Quantity")
    .groupBy("GenreName")
    .agg(F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("revenue"))
    .orderBy(F.desc("revenue")))

revenue_by_genre.show(10)

# COMMAND ----------

sales_by_country = (invoice
    .groupBy("BillingCountry")
    .agg(F.sum("Total").alias("total_sales"),
         F.countDistinct("CustomerId").alias("customers"))
    .orderBy(F.desc("total_sales")))

sales_by_country.show(10)

# COMMAND ----------

top_artists = (invoice_line
    .join(track, "TrackId")
    .join(album, "AlbumId")
    .join(artist, "ArtistId")
    .select(artist["Name"].alias("ArtistName"), "Quantity")
    .groupBy("ArtistName")
    .agg(F.sum("Quantity").alias("tracks_sold"))
    .orderBy(F.desc("tracks_sold"))
    .limit(10))

top_artists.show()

# COMMAND ----------

revenue_by_genre.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{login}.revenue_by_genre")

sales_by_country.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{login}.sales_by_country")

top_artists.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{login}.top_artists")