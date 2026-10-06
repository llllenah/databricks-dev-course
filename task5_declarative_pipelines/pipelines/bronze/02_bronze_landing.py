# Bronze pipeline: JSON files landed in a volume, ingested with Auto Loader.
# Raw data only: no casting or cleaning at this layer.

from pyspark import pipelines as dp

LANDING_PATH = spark.conf.get(
    "landing_path",
    "/Volumes/dbr_dev_ua5816bd/lena066636_bronze/landing/cities/",
)


@dp.table(
    name="cities_landing",
    comment="Raw city records (city_id, city, population, effective_date) landed as JSON files.",
)
def cities_landing():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(LANDING_PATH)
    )
