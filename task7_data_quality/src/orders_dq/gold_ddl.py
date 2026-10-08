from .quality.rules import ACCEPTED_CHANNELS, ACCEPTED_SEGMENTS, MAX_AMOUNT, MIN_AMOUNT


def _in_list(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


TABLES = {
    "dim_date": """
        date_key INT NOT NULL,
        date DATE NOT NULL,
        year INT NOT NULL,
        quarter INT,
        month INT,
        month_name STRING,
        day INT,
        day_name STRING,
        is_weekend BOOLEAN
    """,
    "dim_time": """
        time_key INT NOT NULL,
        hour_of_day INT NOT NULL,
        hour_label STRING,
        day_part STRING NOT NULL,
        is_business_hours BOOLEAN
    """,
    "dim_customer": """
        customer_key INT NOT NULL,
        customer_name STRING NOT NULL,
        segment STRING NOT NULL
    """,
    "fact_orders": """
        order_id INT NOT NULL,
        date_key INT NOT NULL,
        time_key INT NOT NULL,
        customer_key INT NOT NULL,
        segment STRING NOT NULL,
        channel STRING NOT NULL,
        amount DECIMAL(10,2) NOT NULL
    """,
    "agg_daily_sales": """
        date_key INT NOT NULL,
        orders BIGINT NOT NULL,
        revenue DECIMAL(18,2) NOT NULL,
        avg_order_value DECIMAL(10,2)
    """,
    "agg_channel_sales": """
        date_key INT NOT NULL,
        channel STRING NOT NULL,
        orders BIGINT NOT NULL,
        revenue DECIMAL(18,2) NOT NULL
    """,
}

CHECK_CONSTRAINTS = {
    "dim_date": {"date_key_format": "date_key BETWEEN 19000101 AND 29991231"},
    "dim_time": {
        "hour_range": "hour_of_day BETWEEN 0 AND 23",
        "day_part_accepted": "day_part IN ('night', 'morning', 'afternoon', 'evening')",
    },
    "dim_customer": {"segment_accepted": f"segment IN ({_in_list(ACCEPTED_SEGMENTS)})"},
    "fact_orders": {
        "amount_range": f"amount BETWEEN {MIN_AMOUNT} AND {MAX_AMOUNT}",
        "time_key_range": "time_key BETWEEN 0 AND 23",
        "date_key_format": "date_key BETWEEN 19000101 AND 29991231",
        "channel_accepted": f"channel IN ({_in_list(ACCEPTED_CHANNELS)})",
        "segment_accepted": f"segment IN ({_in_list(ACCEPTED_SEGMENTS)})",
    },
    "agg_daily_sales": {"orders_positive": "orders > 0", "revenue_not_negative": "revenue >= 0"},
    "agg_channel_sales": {"orders_positive": "orders > 0", "revenue_not_negative": "revenue >= 0"},
}

COMMENTS = {
    "dim_date": "Dimension: calendar day. Loaded once for several years, not rebuilt with the facts.",
    "dim_time": "Dimension: hour of the day with the day part.",
    "dim_customer": "Dimension: customer with a spend-based segment (premium, standard).",
    "fact_orders": "Fact: one row per valid order. No raw timestamp, only date_key and time_key.",
    "agg_daily_sales": "Aggregation: orders, revenue and average order value per day.",
    "agg_channel_sales": "Aggregation: orders and revenue per day and channel.",
}


def create_table_sql(full_name: str, table: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {full_name} ({TABLES[table].strip()}) "
        f"COMMENT '{COMMENTS[table]}'"
    )


def add_constraint_sql(full_name: str, name: str, expr: str) -> str:
    return f"ALTER TABLE {full_name} ADD CONSTRAINT {name} CHECK ({expr})"


def ensure_table(spark, schema_fqn: str, table: str) -> None:
    full_name = f"{schema_fqn}.{table}"
    spark.sql(create_table_sql(full_name, table))
    existing = {
        r["key"].split(".")[-1]
        for r in spark.sql(f"SHOW TBLPROPERTIES {full_name}").collect()
        if r["key"].startswith("delta.constraints.")
    }
    for name, expr in CHECK_CONSTRAINTS.get(table, {}).items():
        if name.lower() not in existing:
            spark.sql(add_constraint_sql(full_name, name, expr))


def overwrite_table(spark, df, full_name: str) -> None:
    columns = spark.table(full_name).columns
    df.select(*columns).write.insertInto(full_name, overwrite=True)
