import requests
import time
from pyspark.sql import Row

catalog = "dbr_dev_ua5816bd"
login = "lena066636"

top_artists = spark.table(f"{catalog}.{login}.top_artists")
top_artist_names = [r["ArtistName"] for r in top_artists.select("ArtistName").collect()]

# COMMAND ----------

rows = []
for name in top_artist_names:
    resp = requests.get(
        "https://musicbrainz.org/ws/2/artist/",
        params={"query": name, "fmt": "json", "limit": 1},
        headers={"User-Agent": "kpi-databricks-course/1.0 (student project)"}
    )
    data = resp.json()
    if data.get("artists"):
        a = data["artists"][0]
        rows.append(Row(
            artist=name,
            mb_country=a.get("country"),
            mb_type=a.get("type"),
            mb_score=int(a.get("score", 0))
        ))
    else:
        rows.append(Row(artist=name, mb_country=None, mb_type=None, mb_score=0))
    time.sleep(1)

df_enriched = spark.createDataFrame(rows)
df_enriched.show()

# COMMAND ----------

df_enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{login}.artist_enriched")