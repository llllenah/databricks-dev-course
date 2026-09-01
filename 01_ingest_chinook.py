spark.sql("SHOW CATALOGS").show(20, truncate=False)

# COMMAND ----------

catalog = "dbr_dev_ua5816bd"
login = "lena066636"

spark.sql(f"SHOW SCHEMAS IN {catalog}").show()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{login}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {login}")

# COMMAND ----------

import sqlite3
import requests
import pandas as pd

url = "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sql"
sql_script = requests.get(url).text

conn = sqlite3.connect(":memory:")
conn.executescript(sql_script)

tables = ["Artist", "Album", "Track", "Genre", "MediaType",
          "Customer", "Invoice", "InvoiceLine", "Employee",
          "Playlist", "PlaylistTrack"]

for t in tables:
    pdf = pd.read_sql(f"SELECT * FROM {t}", conn)
    sdf = spark.createDataFrame(pdf)
    (sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{login}.{t.lower()}"))
    print(t, "->", sdf.count(), "рядків")


# COMMAND ----------

spark.sql(f"SHOW TABLES IN {catalog}.{login}").show(20, truncate=False)