# Silver pipeline: cleaned city records with data-quality expectations.
# Reads the bronze table from the bronze schema (a separate pipeline writes it).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "dbr_dev_ua5816bd")
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "lena066636_bronze")


@dp.table(
    name="cities_clean",
    comment="Typed and validated city records, with data-quality expectations applied.",
)
@dp.expect_or_drop("valid_city_id", "city_id IS NOT NULL")
@dp.expect_or_drop("valid_city", "city IS NOT NULL AND length(city) > 0")
@dp.expect("population_positive", "population IS NULL OR population > 0")
def cities_clean():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.cities_landing")
        .select(
            F.col("city_id").cast("int").alias("city_id"),
            F.trim(F.col("city")).alias("city"),
            F.col("population").cast("long").alias("population"),
            F.to_date(F.col("effective_date")).alias("effective_date"),
        )
    )

# expect_or_drop: violating rows are dropped and counted in the pipeline's quality metrics.
# expect: violating rows are kept, but the violation is still reported in the metrics.
