"""Simulate app user events (with intentional dirty data) and send them to Kafka.

Examples:
    python generator/produce_events.py --mode backfill --days 7
    python generator/produce_events.py --mode live --rate 20
    python generator/produce_events.py --mode backfill --days 1 --dry-run
"""

import argparse
import json
import os
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "app_events")

CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "books"]
COUNTRIES = ["TH", "SG", "JP", "VN", "MY", "ID"]
COUNTRY_WEIGHTS = [0.45, 0.15, 0.1, 0.1, 0.1, 0.1]
DEVICES = ["ios", "android", "web"]
DEVICE_WEIGHTS = [0.4, 0.4, 0.2]

# Dirty data rates
DUPLICATE_RATE = 0.02
NULL_USER_RATE = 0.01
LATE_RATE = 0.01
MESSY_CASE_RATE = 0.05
MESSY_TS_RATE = 0.20  # share of events NOT in ISO format

AVG_EVENTS_PER_SESSION = 6


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="App event generator -> Kafka")
    parser.add_argument("--mode", choices=["backfill", "live"], default="backfill")
    parser.add_argument("--days", type=int, default=7, help="backfill: number of past days")
    parser.add_argument("--events-per-day", type=int, default=5000, help="backfill: approx events per day")
    parser.add_argument("--users", type=int, default=500)
    parser.add_argument("--items", type=int, default=200)
    parser.add_argument("--rate", type=float, default=10.0, help="live: events per second")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dry-run", action="store_true", help="print events instead of sending")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
def new_id(rng: random.Random):
    """UUID4 that is reproducible with the seed (uuid.uuid4() ignores random.seed)."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def build_items(n: int, rng: random.Random):
    return {
        f"item_{i:04d}": {
            "category": rng.choice(CATEGORIES),
            "price": round(rng.uniform(5, 300), 2),
        }
        for i in range(n)
    }


def build_users(n: int, days: int, rng: random.Random):
    """Each user has a signup_day so new users keep appearing (needed for D7 retention)."""
    return [
        {
            "user_id": f"user_{i:05d}",
            "country": rng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0],
            "device": rng.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
            "signup_day": rng.randrange(max(days, 1)),
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Event simulation
# ---------------------------------------------------------------------------
def make_event(event_type: str, user: dict, session_id: str, ts: datetime, rng: random.Random, item_id: str | None = None, price: float | None = None):
    return {
        "event_id": new_id(rng),
        "user_id": user["user_id"],
        "session_id": session_id,
        "event_type": event_type,
        "item_id": item_id,
        "price": price,
        "device": user["device"],
        "country": user["country"],
        "event_ts": ts,  # datetime for now; formatted in make_dirty()
    }


def simulate_session(user: dict, start_ts: datetime, items: dict, rng: random.Random):
    """One visit following the funnel: app_open -> search -> view -> cart -> checkout -> purchase."""
    session_id = new_id(rng)
    ts = start_ts
    events = []

    def step(event_type, item_id=None, price=None):
        nonlocal ts
        events.append(make_event(event_type, user, session_id, ts, rng, item_id, price))
        ts += timedelta(seconds=rng.randint(5, 120))

    step("app_open")
    if rng.random() < 0.6:
        step("search")

    viewed = rng.sample(list(items), rng.randint(1, 5))
    for item_id in viewed:
        step("view_item", item_id)

    cart = [item_id for item_id in viewed if rng.random() < 0.3]
    for item_id in cart:
        step("add_to_cart", item_id)

    if cart and rng.random() < 0.15:
        step("remove_from_cart", cart.pop())

    if cart and rng.random() < 0.6:
        step("checkout")
        if rng.random() < 0.8:
            for item_id in cart:
                step("purchase", item_id, items[item_id]["price"])

    return events


# ---------------------------------------------------------------------------
# Dirty data
# ---------------------------------------------------------------------------
def format_ts(ts: datetime, rng: random.Random, stats: Counter) -> str | int:
    r = rng.random()
    if r < 1 - MESSY_TS_RATE:
        return ts.isoformat()
    stats["messy_timestamp"] += 1
    if r < 1 - MESSY_TS_RATE / 2:
        return ts.strftime("%Y/%m/%d %H:%M:%S")
    return int(ts.timestamp())  # epoch seconds


def make_dirty(event: dict, rng: random.Random, stats: Counter):
    """Return a JSON-ready copy of the event with dirty data injected."""
    payload = dict(event)
    payload["event_ts"] = format_ts(event["event_ts"], rng, stats)

    if rng.random() < NULL_USER_RATE:
        payload["user_id"] = None
        stats["null_user_id"] += 1

    if rng.random() < MESSY_CASE_RATE:
        payload["event_type"] = rng.choice([str.upper, str.title])(payload["event_type"])
        stats["messy_casing"] += 1

    return payload


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
def create_producer(dry_run: bool):
    if dry_run:
        return None
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,  # no broker-side duplicates from retries
        "linger.ms": 50,             # small wait to batch messages
    })


def make_delivery_callback(stats: Counter):
    def on_delivery(err, msg):
        if err is not None:
            stats["delivery_failed"] += 1
    return on_delivery


def send(producer, payload: dict, true_ts: datetime, stats: Counter, on_delivery, dry_run: bool):
    value = json.dumps(payload)
    stats["sent"] += 1
    if dry_run:
        if stats["sent"] <= 5:
            print(value)
        return

    while True:
        try:
            producer.produce(
                TOPIC,
                key=payload["user_id"],                    # same user -> same partition (ordering)
                value=value,
                timestamp=int(true_ts.timestamp() * 1000),  # Kafka timestamp = real event time (ms)
                on_delivery=on_delivery,
            )
            break
        except BufferError:
            producer.poll(1)  # local queue full: wait for deliveries, then retry
    producer.poll(0)


def emit(producer, event: dict, rng: random.Random, stats: Counter, on_delivery, dry_run: bool, late_buffer: list | None):
    """Dirty the event, then send it (sometimes twice, sometimes later)."""
    payload = make_dirty(event, rng, stats)
    stats[f"type:{event['event_type']}"] += 1

    if late_buffer is not None and rng.random() < LATE_RATE:
        late_buffer.append((payload, event["event_ts"]))
        stats["late"] += 1
        return

    send(producer, payload, event["event_ts"], stats, on_delivery, dry_run)
    if rng.random() < DUPLICATE_RATE:
        send(producer, payload, event["event_ts"], stats, on_delivery, dry_run)
        stats["duplicate"] += 1


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------
def run_backfill(args, producer, items: dict, users: list, rng, stats, on_delivery):
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    sessions_per_day = max(1, args.events_per_day // AVG_EVENTS_PER_SESSION)
    late_buffer = []

    for day in range(args.days):
        day_start = today - timedelta(days=args.days - day)
        active_users = [u for u in users if u["signup_day"] <= day]
        if not active_users:
            continue

        day_events = []
        for _ in range(sessions_per_day):
            user = rng.choice(active_users)
            start = day_start + timedelta(seconds=rng.randint(0, 86_399))
            day_events.extend(simulate_session(user, start, items, rng))

        day_events.sort(key=lambda e: e["event_ts"])
        for event in day_events:
            emit(producer, event, rng, stats, on_delivery, args.dry_run, late_buffer)
        print(f"day {day + 1}/{args.days} ({day_start.date()}): {len(day_events)} events")

    # Late events arrive after everything else
    for payload, true_ts in late_buffer:
        send(producer, payload, true_ts, stats, on_delivery, args.dry_run)


def run_live(args, producer, items: dict, users: list, rng, stats, on_delivery):
    print(f"Live mode: ~{args.rate} events/sec to '{TOPIC}'. Ctrl+C to stop.")
    try:
        while True:
            user = rng.choice(users)
            for event in simulate_session(user, datetime.now(timezone.utc), items, rng):
                event["event_ts"] = datetime.now(timezone.utc)
                emit(producer, event, rng, stats, on_delivery, args.dry_run, late_buffer=None)
                time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("\nStopping...")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def print_summary(stats: Counter, elapsed: float):
    print("\n===== Generator summary =====")
    print(f"messages sent    : {stats['sent']:,}  ({elapsed:.1f}s)")
    print(f"duplicates       : {stats['duplicate']:,}")
    print(f"null user_id     : {stats['null_user_id']:,}")
    print(f"late events      : {stats['late']:,}")
    print(f"messy timestamps : {stats['messy_timestamp']:,}")
    print(f"messy casing     : {stats['messy_casing']:,}")
    print(f"delivery failed  : {stats['delivery_failed']:,}")
    print("events by type   :")
    for key, count in sorted(stats.items()):
        if key.startswith("type:"):
            print(f"  {key[5:]:<17}{count:,}")


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    stats = Counter()
    on_delivery = make_delivery_callback(stats)

    items = build_items(args.items, rng)
    users = build_users(args.users, args.days, rng)
    producer = create_producer(args.dry_run)

    start = time.monotonic()
    try:
        if args.mode == "backfill":
            run_backfill(args, producer, items, users, rng, stats, on_delivery)
        else:
            run_live(args, producer, items, users, rng, stats, on_delivery)
    finally:
        if producer is not None:
            remaining = producer.flush(30)
            if remaining:
                print(f"WARNING: {remaining} messages not delivered")
        print_summary(stats, time.monotonic() - start)


if __name__ == "__main__":
    main()