from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer", StringType()),
        StructField("channel", StringType()),
        StructField("amount", StringType()),
        StructField("ts", StringType()),
    ]
)

TS_FORMATS = (
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "dd.MM.yyyy HH:mm:ss",
)

PREMIUM_SHARE_BUCKETS = 5


def parse_order_ts(col: Column) -> Column:
    value = F.trim(col)
    return F.coalesce(*[F.try_to_timestamp(value, F.lit(fmt)) for fmt in TS_FORMATS])


def parse_orders(raw: DataFrame) -> DataFrame:
    extra = [c for c in ("_source_file", "_ingested_at") if c in raw.columns]
    return raw.select(
        F.expr("try_cast(trim(order_id) AS INT)").alias("order_id"),
        F.lower(F.trim("customer")).alias("customer"),
        F.lower(F.trim("channel")).alias("channel"),
        F.expr("try_cast(trim(amount) AS DECIMAL(10,2))").alias("amount"),
        parse_order_ts(F.col("ts")).alias("order_ts"),
        *extra,
    )


def date_key(ts: Column) -> Column:
    return F.date_format(F.to_date(ts), "yyyyMMdd").cast("int")


def time_key(ts: Column) -> Column:
    return F.hour(ts).cast("int")


def day_part(hour: Column) -> Column:
    return (
        F.when(hour < 6, "night")
        .when(hour < 12, "morning")
        .when(hour < 18, "afternoon")
        .otherwise("evening")
    )


def amount_bucket(amount: Column) -> Column:
    return (
        F.when(amount < 50, "1) under 50")
        .when(amount < 100, "2) 50-100")
        .when(amount < 150, "3) 100-150")
        .otherwise("4) 150+")
    )


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    days = spark.sql(
        f"SELECT explode(sequence(to_date('{start.isoformat()}'), to_date('{end.isoformat()}'), interval 1 day)) AS date"
    )
    return days.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_time(spark: SparkSession) -> DataFrame:
    hours = spark.range(0, 24).select(F.col("id").cast("int").alias("hour_of_day"))
    padded = F.lpad(F.col("hour_of_day").cast("string"), 2, "0")
    return hours.select(
        F.col("hour_of_day").alias("time_key"),
        "hour_of_day",
        F.concat(padded, F.lit(":00-"), padded, F.lit(":59")).alias("hour_label"),
        day_part(F.col("hour_of_day")).alias("day_part"),
        F.col("hour_of_day").between(9, 17).alias("is_business_hours"),
    )


def build_dim_customer(orders: DataFrame) -> DataFrame:
    spend = orders.groupBy("customer").agg(F.sum("amount").alias("total_spend"))
    return (
        spend.withColumn("customer_key", F.row_number().over(Window.orderBy("customer")).cast("int"))
        .withColumn(
            "spend_bucket",
            F.ntile(PREMIUM_SHARE_BUCKETS).over(Window.orderBy(F.col("total_spend").desc(), F.col("customer"))),
        )
        .withColumn("segment", F.when(F.col("spend_bucket") == 1, "premium").otherwise("standard"))
        .select("customer_key", F.col("customer").alias("customer_name"), "segment")
    )


def build_fact_orders(orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    return (
        orders.alias("o")
        .join(dim_customer.alias("c"), F.col("o.customer") == F.col("c.customer_name"), "left")
        .select(
            F.col("o.order_id").alias("order_id"),
            date_key(F.col("o.order_ts")).alias("date_key"),
            time_key(F.col("o.order_ts")).alias("time_key"),
            F.col("c.customer_key").alias("customer_key"),
            F.col("c.segment").alias("segment"),
            F.col("o.channel").alias("channel"),
            F.col("o.amount").alias("amount"),
        )
    )


def build_agg_daily_sales(fact: DataFrame) -> DataFrame:
    return fact.groupBy("date_key").agg(
        F.count("*").alias("orders"),
        F.sum("amount").cast("decimal(18,2)").alias("revenue"),
        F.round(F.avg("amount"), 2).cast("decimal(10,2)").alias("avg_order_value"),
    )


def build_agg_channel_sales(fact: DataFrame) -> DataFrame:
    return fact.groupBy("date_key", "channel").agg(
        F.count("*").alias("orders"),
        F.sum("amount").cast("decimal(18,2)").alias("revenue"),
    )
