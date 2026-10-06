# Silver pipeline: SCD Type 2 dimension of cities, built with apply_changes.
# A new version of a city is created when its city name or population changes.
# effective_date is excluded from change tracking: it is the sequence column, so a repeated
# record with an unchanged population must not create a new version.

from pyspark import pipelines as dp

dp.create_streaming_table("cities_scd2")

dp.apply_changes(
    target="cities_scd2",
    source="cities_clean",
    keys=["city_id"],
    sequence_by="effective_date",
    stored_as_scd_type=2,
    track_history_except_column_list=["effective_date"],
)
