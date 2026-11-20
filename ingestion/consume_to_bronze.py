"""Consume raw app events from Kafka and append them to the bronze layer in DuckLake.

Bronze rules:
  * store everything as received (event fields as strings + original JSON)
  * never drop a message, even if it is not valid JSON (flag it instead)
  * commit Kafka offsets only AFTER the batch is safely written (at-least-once)

Examples:
    python ingestion/consume_to_bronze.py --exit-when-idle 15   # load everything, then stop
    python ingestion/consume_to_bronze.py                        # run forever (Ctrl+C to stop)
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from pyducklake import (
    BigIntType,
    BooleanType,
    Catalog,
    DateType,
    IntegerType,
    Schema,
    StringType,
    TimestampTZType,
    optional,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "app_events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bronze-loader")

LAKE_DIR = os.getenv("LAKE_DIR", "lakehouse")
CATALOG_NAME = "lake"
CATALOG_URI = os.path.join(LAKE_DIR, "catalog.ducklake")  # DuckDB file as the metadata catalog
DATA_PATH = os.path.join(LAKE_DIR, "data") + "/"          # Parquet files go here
TABLE_ID = ("bronze", "raw_events")

EVENT_FIELDS = [
    "event_id", "user_id", "session_id", "event_type", "item_id",
    "price", "device", "country", "event_ts",
]

# All columns optional: bronze must accept NULLs and messy values.
# Schema.of() auto-assigns field IDs (plain Schema(...) needs explicit IDs).
BRONZE_SCHEMA = Schema.of(
    *[optional(name, StringType()) for name in EVENT_FIELDS],
    optional("raw_payload", StringType()),
    optional("parse_error", BooleanType()),
    optional("kafka_partition", IntegerType()),
    optional("kafka_offset", BigIntType()),
    optional("kafka_timestamp", TimestampTZType()),
    optional("ingest_ts", TimestampTZType()),
    optional("ingest_date", DateType()),
)
# Arrow schema generated from the DuckLake schema -> same column order and types.
ARROW_SCHEMA = BRONZE_SCHEMA.as_arrow()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Kafka -> DuckLake bronze loader")
    parser.add_argument("--batch-size", type=int, default=5000, help="flush after N rows")
    parser.add_argument("--flush-seconds", type=float, default=30, help="flush after N seconds")
    parser.add_argument("--exit-when-idle", type=float, default=0,
                        help="stop after N seconds with no new messages (0 = run forever)")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# DuckLake
# ---------------------------------------------------------------------------
def open_catalog():
    os.makedirs(DATA_PATH, exist_ok=True)
    return Catalog(CATALOG_NAME, CATALOG_URI, data_path=DATA_PATH)


def ensure_bronze_table(catalog: Catalog):
    namespace, _ = TABLE_ID
    if not catalog.namespace_exists(namespace):
        catalog.create_namespace(namespace)
        print(f"created namespace '{namespace}'")
    if not catalog.table_exists(TABLE_ID):
        catalog.create_table(TABLE_ID, BRONZE_SCHEMA)
        print(f"created table '{'.'.join(TABLE_ID)}'")


def describe_offsets(rows: list[dict]):
    """'p0:120-450 p1:98-400' -> used in the commit message for traceability."""
    ranges = {}
    for row in rows:
        p, o = row["kafka_partition"], row["kafka_offset"]
        lo, hi = ranges.get(p, (o, o))
        ranges[p] = (min(lo, o), max(hi, o))
    return " ".join(f"p{p}:{lo}-{hi}" for p, (lo, hi) in sorted(ranges.items()))


def write_batch(catalog: Catalog, rows: list[dict]):
    table = pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)
    message = f"bronze: +{len(rows)} events | {describe_offsets(rows)}"
    with catalog.begin_transaction() as txn:
        catalog.set_commit_message(message, author="consume_to_bronze")
        txn.load_table(TABLE_ID).append(table)
    return message


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
def create_consumer():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # new group -> start from the beginning
        "enable.auto.commit": False,      # we commit manually after writing
    })


def to_text(value):
    return None if value is None else str(value)


def to_row(msg: confluent_kafka.Message):
    """Kafka message -> one bronze row. Never raises on bad payloads."""
    raw = msg.value().decode("utf-8", errors="replace") if msg.value() is not None else None

    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("payload is not a JSON object")
        parse_error = False
    except (TypeError, ValueError):  # JSONDecodeError is a ValueError
        data, parse_error = {}, True

    _, ts_ms = msg.timestamp()
    kafka_ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if ts_ms and ts_ms > 0 else None
    now = datetime.now(timezone.utc)

    row = {field: to_text(data.get(field)) for field in EVENT_FIELDS}
    row.update(
        raw_payload=raw,
        parse_error=parse_error,
        kafka_partition=msg.partition(),
        kafka_offset=msg.offset(),
        kafka_timestamp=kafka_ts,
        ingest_ts=now,
        ingest_date=now.date(),
    )
    return row


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    catalog = open_catalog()
    ensure_bronze_table(catalog)

    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    print(f"consuming '{TOPIC}' as group '{GROUP_ID}' -> {CATALOG_NAME}.{'.'.join(TABLE_ID)}")

    buffer = []
    total = 0
    last_flush = last_message = time.monotonic()

    def flush():
        nonlocal total, last_flush
        if buffer:
            message = write_batch(catalog, buffer)       # 1) write to DuckLake
            consumer.commit(asynchronous=False)         # 2) then commit offsets
            total += len(buffer)
            print(f"{message} (total {total:,})")
            buffer.clear()
        last_flush = time.monotonic()

    try:
        while True:
            messages = consumer.consume(num_messages=1000, timeout=1.0)
            now = time.monotonic()
            if messages:
                last_message = now

            for msg in messages:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"kafka error: {msg.error()}")
                    continue
                buffer.append(to_row(msg))

            if len(buffer) >= args.batch_size or now - last_flush >= args.flush_seconds:
                flush()

            if args.exit_when_idle and now - last_message >= args.exit_when_idle:
                print(f"no new messages for {args.exit_when_idle:.0f}s, stopping")
                break
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        flush()
        consumer.close()
        print(f"done: {total:,} events written to bronze")


if __name__ == "__main__":
    main()