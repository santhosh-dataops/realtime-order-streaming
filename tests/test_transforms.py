import json
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from producer.order_producer import generate
from streaming.transforms import (
    enrich,
    parse_kafka_value,
    split_valid_invalid,
    windowed_revenue,
)


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def kafka_frame(spark, events):
    rows = [
        (e.get("order_id", "x").encode(), json.dumps(e).encode(), "orders.events.v1", 0, i, datetime(2026, 1, 1))
        for i, e in enumerate(events)
    ]
    return spark.createDataFrame(rows, "key binary, value binary, topic string, partition int, offset long, timestamp timestamp")


def event(event_id, order_id, etype, ts, qty=2, price=10.0, region=" us-east "):
    return {
        "event_id": event_id,
        "order_id": order_id,
        "customer_id": "cust-1",
        "event_type": etype,
        "product_category": "home",
        "quantity": qty,
        "unit_price": price,
        "region": region,
        "event_ts": ts,
    }


def test_parse_and_route_invalid(spark):
    events = [
        event("e1", "o1", "paid", "2026-01-01T10:00:00Z"),
        event("e2", "o2", "refunded", "2026-01-01T10:00:00Z"),
        event("e3", "o3", "paid", "2026-01-01T10:00:00Z", qty=-1),
    ]
    valid, invalid = split_valid_invalid(parse_kafka_value(kafka_frame(spark, events)))
    assert [r.event_id for r in valid.collect()] == ["e1"]
    assert sorted(r.event_id for r in invalid.collect()) == ["e2", "e3"]


def test_unparseable_json_goes_to_dlq(spark):
    rows = [(b"k", b"{not json", "t", 0, 0, datetime(2026, 1, 1))]
    df = spark.createDataFrame(rows, "key binary, value binary, topic string, partition int, offset long, timestamp timestamp")
    valid, invalid = split_valid_invalid(parse_kafka_value(df))
    assert valid.count() == 0 and invalid.count() == 1


def test_enrich_computes_amount_and_normalises_region(spark):
    events = [event("e1", "o1", "paid", "2026-01-01T10:00:00Z", qty=3, price=9.99)]
    valid, _ = split_valid_invalid(parse_kafka_value(kafka_frame(spark, events)))
    row = enrich(valid).collect()[0]
    assert row.gross_amount == pytest.approx(29.97)
    assert row.region == "US-EAST"
    assert str(row.event_date) == "2026-01-01"


def test_windowed_revenue_only_counts_paid(spark):
    events = [
        event("e1", "o1", "paid", "2026-01-01T10:01:00Z", qty=1, price=100.0),
        event("e2", "o2", "paid", "2026-01-01T10:03:00Z", qty=2, price=50.0),
        event("e3", "o3", "created", "2026-01-01T10:02:00Z", qty=9, price=999.0),
        event("e4", "o4", "paid", "2026-01-01T10:07:00Z", qty=1, price=20.0),
    ]
    valid, _ = split_valid_invalid(parse_kafka_value(kafka_frame(spark, events)))
    agg = windowed_revenue(enrich(valid)).withColumn(
        "ws", F.date_format("window_start", "yyyy-MM-dd HH:mm:ss")
    )
    out = {(r.ws, r.region): r for r in agg.collect()}
    first = out[("2026-01-01 10:00:00", "US-EAST")]
    assert first.paid_orders == 2 and first.revenue == 200.0
    assert out[("2026-01-01 10:05:00", "US-EAST")].revenue == 20.0


def test_producer_emits_lifecycle_events():
    evs = list(generate(50, late_pct=0, dup_pct=0, bad_pct=0))
    assert {e["event_type"] for e in evs} <= {"created", "paid", "shipped", "cancelled"}
    assert all("event_ts" in e for e in evs)
