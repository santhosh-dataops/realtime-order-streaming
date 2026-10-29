"""Pure transformation logic for the order-event stream.

Every function takes and returns a DataFrame so the same code runs on a
streaming DataFrame in production and on a static DataFrame in unit tests.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("event_type", StringType(), False),  # created | paid | shipped | cancelled
        StructField("product_category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("region", StringType(), True),
        StructField("event_ts", TimestampType(), False),
    ]
)

VALID_EVENT_TYPES = ["created", "paid", "shipped", "cancelled"]


def parse_kafka_value(raw: DataFrame) -> DataFrame:
    """Decode the Kafka `value` bytes into typed columns, keeping Kafka metadata."""
    return (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("payload"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("data", F.from_json("payload", ORDER_EVENT_SCHEMA))
        .select("kafka_key", "payload", "topic", "partition", "offset", "kafka_ts", "data.*")
    )


def split_valid_invalid(events: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Route malformed events to a dead-letter frame instead of failing the stream."""
    rule = (
        F.col("event_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & F.col("event_type").isin(VALID_EVENT_TYPES)
        & (F.coalesce(F.col("quantity"), F.lit(0)) >= 0)
        & (F.coalesce(F.col("unit_price"), F.lit(0.0)) >= 0)
    )
    flagged = events.withColumn("_is_valid", F.coalesce(rule, F.lit(False)))
    valid = flagged.filter("_is_valid").drop("_is_valid")
    invalid = (
        flagged.filter(~F.col("_is_valid"))
        .drop("_is_valid")
        .withColumn("dlq_reason", F.lit("schema_or_rule_violation"))
        .withColumn("dlq_ts", F.current_timestamp())
    )
    return valid, invalid


def enrich(valid: DataFrame) -> DataFrame:
    """Add derived business columns."""
    return (
        valid.withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("region", F.upper(F.trim(F.col("region"))))
    )


def deduplicate(enriched: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    """Drop replayed events (at-least-once producers) within the watermark window."""
    return enriched.withWatermark("event_ts", watermark).dropDuplicates(["event_id", "event_ts"])


def windowed_revenue(
    events: DataFrame, window: str = "5 minutes", watermark: str = "10 minutes"
) -> DataFrame:
    """Tumbling-window revenue and order counts per region for paid orders."""
    return (
        events.filter(F.col("event_type") == "paid")
        .withWatermark("event_ts", watermark)
        .groupBy(F.window("event_ts", window).alias("w"), "region")
        .agg(
            F.countDistinct("order_id").alias("paid_orders"),
            F.round(F.sum("gross_amount"), 2).alias("revenue"),
            F.round(F.avg("gross_amount"), 2).alias("avg_order_value"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "region",
            "paid_orders",
            "revenue",
            "avg_order_value",
        )
    )
