# Bronze pipeline: streaming source (Event Hub via its Kafka-compatible endpoint).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

EVENTHUB_NAMESPACE = spark.conf.get("eventhub_namespace", "evhua5816bd")
EVENTHUB_NAME = spark.conf.get("eventhub_name", "crypto-ticks")
SECRET_SCOPE = spark.conf.get("secret_scope", "team-crypto-scope")


@dp.table(
    name="ticks_stream",
    comment="Raw crypto ticks ingested from Event Hub via the Kafka-compatible endpoint.",
)
def ticks_stream():
    connection_str = dbutils.secrets.get(scope=SECRET_SCOPE, key="eventhub-connection-string")
    jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_str}";'
    )
    kafka_options = {
        "kafka.bootstrap.servers": f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093",
        "subscribe": EVENTHUB_NAME,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": jaas_config,
        "startingOffsets": "earliest",
    }
    return (
        spark.readStream.format("kafka").options(**kafka_options).load()
        .withColumn("value", F.col("value").cast("string"))
        .withColumn("source", F.lit("eventhub"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select("value", "source", "ingestion_timestamp")
    )
