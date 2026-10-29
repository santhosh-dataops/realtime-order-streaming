"""Synthetic order-event producer for Kafka.

Simulates an e-commerce order lifecycle (created -> paid -> shipped, with some
cancellations), plus a small share of late, duplicate and malformed events so
the streaming job's watermarking, dedup and DLQ paths are exercised.

    python src/producer/order_producer.py --rate 200 --duration 300
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

CATEGORIES = ["electronics", "home", "apparel", "grocery", "toys", "beauty"]
REGIONS = ["us-east", "us-west", "us-central", "eu-west", "apac"]
PRICE_RANGE = {
    "electronics": (40, 1200),
    "home": (10, 400),
    "apparel": (8, 150),
    "grocery": (2, 60),
    "toys": (5, 120),
    "beauty": (4, 90),
}


def make_order() -> dict:
    category = random.choice(CATEGORIES)
    lo, hi = PRICE_RANGE[category]
    return {
        "order_id": f"ord-{uuid.uuid4().hex[:12]}",
        "customer_id": f"cust-{random.randint(1, 50_000):06d}",
        "product_category": category,
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(lo, hi), 2),
        "region": random.choice(REGIONS),
    }


def lifecycle(order: dict, now: datetime) -> list[dict]:
    steps = ["created", "paid", "shipped"] if random.random() > 0.08 else ["created", "cancelled"]
    events, ts = [], now
    for step in steps:
        ts += timedelta(seconds=random.randint(1, 90))
        events.append({**order, "event_id": str(uuid.uuid4()), "event_type": step, "event_ts": ts.isoformat()})
    return events


def corrupt(event: dict) -> dict:
    bad = dict(event)
    choice = random.choice(["neg_qty", "bad_type", "no_ts"])
    if choice == "neg_qty":
        bad["quantity"] = -1
    elif choice == "bad_type":
        bad["event_type"] = "refunded???"
    else:
        bad.pop("event_ts")
    return bad


def generate(n_orders: int, late_pct=0.02, dup_pct=0.01, bad_pct=0.005):
    now = datetime.now(timezone.utc)
    for _ in range(n_orders):
        for ev in lifecycle(make_order(), now):
            r = random.random()
            if r < bad_pct:
                yield corrupt(ev)
                continue
            if r < bad_pct + late_pct:
                late = datetime.fromisoformat(ev["event_ts"]) - timedelta(minutes=random.randint(3, 20))
                ev["event_ts"] = late.isoformat()
            yield ev
            if random.random() < dup_pct:
                yield ev  # at-least-once replay


def main() -> None:
    from kafka import KafkaProducer  # imported lazily so tests don't need kafka-python

    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--topic", default="orders.events.v1")
    p.add_argument("--rate", type=int, default=100, help="orders per second")
    p.add_argument("--duration", type=int, default=60, help="seconds")
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
        linger_ms=20,
        compression_type="gzip",
    )
    end = time.time() + args.duration
    sent = 0
    while time.time() < end:
        tick = time.time()
        for ev in generate(args.rate):
            producer.send(args.topic, key=ev.get("order_id", "unknown"), value=ev)
            sent += 1
        producer.flush()
        time.sleep(max(0.0, 1 - (time.time() - tick)))
    print(f"sent {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
