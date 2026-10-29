# Real-Time Order Streaming Pipeline

Kafka → Spark Structured Streaming → Delta Lake pipeline that turns raw e-commerce order events into a clean, deduplicated event table and near-real-time revenue metrics.

![CI](https://github.com/GITHUB_USERNAME/realtime-order-streaming/actions/workflows/ci.yml/badge.svg)

## Architecture

```
 order_producer.py ──► Kafka topic: orders.events.v1 (6 partitions)
                                │
                     Spark Structured Streaming
                                │
         ┌──────────────────────┼──────────────────────────┐
         ▼                      ▼                          ▼
  silver/order_events    dlq/order_events     gold/revenue_by_region_5m
  (Delta, by event_date) (malformed events)   (5-min tumbling windows)
```

## What it demonstrates

| Concern | How it's handled |
|---|---|
| Schema enforcement | Explicit `StructType`; `from_json` with typed columns |
| Bad data | Rule-based validation routes failures to a Delta **dead-letter queue** instead of crashing the stream |
| Duplicates | At-least-once replays dropped with `dropDuplicates` inside a 10-minute watermark |
| Late data | Event-time watermarking; producer deliberately emits events up to 20 minutes late |
| Exactly-once sinks | Per-query checkpoints + Delta transactional writes |
| Back-pressure | `maxOffsetsPerTrigger` caps micro-batch size |
| Testability | All logic lives in pure `DataFrame -> DataFrame` functions, unit-tested with static frames |

## Run it locally

```bash
docker compose up -d                       # Kafka (KRaft), topic init, Spark job
pip install -r requirements.txt
python src/producer/order_producer.py --rate 200 --duration 300
```

Query results from the Spark container:

```python
spark.read.format("delta").load("/data/lake/gold/revenue_by_region_5m") \
     .orderBy("window_start", ascending=False).show()
```

## Tests

```bash
pip install -r requirements-dev.txt
PYTHONPATH=src pytest -q
```

## Project layout

```
src/producer/order_producer.py   synthetic events incl. late / duplicate / malformed
src/streaming/transforms.py      parse, validate, enrich, dedup, window aggregates
src/streaming/job.py             streaming queries and Delta sinks
tests/                           PySpark unit tests
docker-compose.yml               Kafka + Spark local stack
```

## Tech
Python · Apache Kafka · Spark Structured Streaming · Delta Lake · Docker · PyTest · GitHub Actions
