"""Spark Structured Streaming job: Kafka -> Delta Lake (bronze events, DLQ, gold aggregates).

Run (inside the docker-compose spark container):
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0 \
      src/streaming/job.py
"""
import os

from pyspark.sql import SparkSession

from streaming.transforms import (
    deduplicate,
    enrich,
    parse_kafka_value,
    split_valid_invalid,
    windowed_revenue,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("ORDERS_TOPIC", "orders.events.v1")
LAKE = os.getenv("LAKE_PATH", "/data/lake")
CHECKPOINTS = os.getenv("CHECKPOINT_PATH", "/data/checkpoints")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("realtime-order-streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = parse_kafka_value(raw)
    valid, invalid = split_valid_invalid(parsed)
    clean = deduplicate(enrich(valid))

    (
        clean.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("event_date")
        .option("checkpointLocation", f"{CHECKPOINTS}/order_events")
        .trigger(processingTime="30 seconds")
        .start(f"{LAKE}/silver/order_events")
    )

    (
        invalid.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/order_events_dlq")
        .start(f"{LAKE}/dlq/order_events")
    )

    (
        windowed_revenue(clean)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/revenue_5m")
        .trigger(processingTime="1 minute")
        .start(f"{LAKE}/gold/revenue_by_region_5m")
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
