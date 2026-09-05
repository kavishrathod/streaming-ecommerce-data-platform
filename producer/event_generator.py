"""
producer/event_generator.py — Phase 2 core deliverable.

Continuously generates simulated e-commerce events and publishes them to
Redpanda, following the locked producer behavior design:

    ~87% normal events
    ~3%  duplicates          (same event_id resent)
    ~2.5% schema violations  (null/missing required field, wrong type)
    ~2.5% business-rule violations (negative amount, invalid status, etc.)
    ~5%  late events         (event_timestamp far behind ingestion_timestamp,
                               biased toward product_returned)

Fault categories are mutually exclusive per event - each generated event
gets exactly one classification, keeping Phase 2 DQ metrics clean and
explainable.

Run:
    python producer/event_generator.py
"""

import json
import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from config import settings as config


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Base event builders — one per event_type, always schema-valid and sane.
# ---------------------------------------------------------------------------

def build_order_created():
    ts = datetime.now(timezone.utc)
    return {
        "event_id": make_id("evt"),
        "event_type": "order_created",
        "order_id": make_id("ord"),
        "customer_id": make_id("cust"),
        "product_id": make_id("prod"),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(50, 2000), 2),
        "total_amount": None,  # filled below
        "region": random.choice(config.REGIONS),
        "event_timestamp": ts.isoformat(),
        "ingestion_timestamp": ts.isoformat(),
    }


def build_payment_completed(order_id: str = None):
    ts = datetime.now(timezone.utc)
    return {
        "event_id": make_id("evt"),
        "event_type": "payment_completed",
        "order_id": order_id or make_id("ord"),
        "payment_id": make_id("pay"),
        "amount": round(random.uniform(50, 2000), 2),
        "payment_method": random.choice(["UPI", "Card", "NetBanking", "COD"]),
        "status": "success",
        "event_timestamp": ts.isoformat(),
        "ingestion_timestamp": ts.isoformat(),
    }


def build_product_viewed():
    ts = datetime.now(timezone.utc)
    return {
        "event_id": make_id("evt"),
        "event_type": "product_viewed",
        "product_id": make_id("prod"),
        "product_name": random.choice(
            ["Wireless Mouse", "USB-C Cable", "Laptop Stand", "Mechanical Keyboard", "Webcam"]
        ),
        "category": random.choice(["Electronics", "Accessories", "Office"]),
        "stock_quantity": random.randint(0, 500),
        "price": round(random.uniform(50, 2000), 2),
        "event_timestamp": ts.isoformat(),
        "ingestion_timestamp": ts.isoformat(),
    }


def build_stock_updated():
    ts = datetime.now(timezone.utc)
    return {
        "event_id": make_id("evt"),
        "event_type": "stock_updated",
        "product_id": make_id("prod"),
        "product_name": random.choice(
            ["Wireless Mouse", "USB-C Cable", "Laptop Stand", "Mechanical Keyboard", "Webcam"]
        ),
        "category": random.choice(["Electronics", "Accessories", "Office"]),
        "stock_quantity": random.randint(0, 500),
        "price": round(random.uniform(50, 2000), 2),
        "event_timestamp": ts.isoformat(),
        "ingestion_timestamp": ts.isoformat(),
    }


def build_product_returned(order_id: str = None, product_id: str = None):
    ts = datetime.now(timezone.utc)
    return {
        "event_id": make_id("evt"),
        "event_type": "product_returned",
        "order_id": order_id or make_id("ord"),
        "product_id": product_id or make_id("prod"),
        "customer_id": make_id("cust"),
        "reason": random.choice(["defective", "wrong_item", "not_needed", "other"]),
        "refund_amount": round(random.uniform(50, 2000), 2),
        "region": random.choice(config.REGIONS),
        "event_timestamp": ts.isoformat(),
        "ingestion_timestamp": ts.isoformat(),
    }


BUILDERS = {
    "order_created": build_order_created,
    "payment_completed": build_payment_completed,
    "product_viewed": build_product_viewed,
    "stock_updated": build_stock_updated,
    "product_returned": build_product_returned,
}

# Required fields per event_type — used to pick a target for schema violations
REQUIRED_FIELDS = {
    "order_created": ["order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount"],
    "payment_completed": ["order_id", "payment_id", "amount", "payment_method", "status"],
    "product_viewed": ["product_id", "product_name", "price"],
    "stock_updated": ["product_id", "product_name", "price"],
    "product_returned": ["order_id", "product_id", "customer_id", "refund_amount"],
}


def pick_event_type() -> str:
    types = list(config.EVENT_TYPE_WEIGHTS.keys())
    weights = list(config.EVENT_TYPE_WEIGHTS.values())
    return random.choices(types, weights=weights, k=1)[0]


def pick_late_event_type() -> str:
    types = list(config.LATE_EVENT_TYPE_BIAS.keys())
    weights = list(config.LATE_EVENT_TYPE_BIAS.values())
    return random.choices(types, weights=weights, k=1)[0]


def finalize_amounts(event: dict) -> dict:
    """order_created needs total_amount derived from quantity * unit_price."""
    if event.get("event_type") == "order_created":
        event["total_amount"] = round(event["quantity"] * event["unit_price"], 2)
    return event


def apply_schema_violation(event: dict) -> dict:
    """Null out or type-corrupt one required field, breaking schema validation on purpose."""
    event_type = event["event_type"]
    field = random.choice(REQUIRED_FIELDS[event_type])
    mutation = random.choice(["null", "wrong_type"])
    if mutation == "null":
        event[field] = None
    else:
        # break the type: turn a number into a string, or a string into a number
        current = event[field]
        if isinstance(current, (int, float)):
            event[field] = "not_a_number"
        else:
            event[field] = 12345
    return event


def apply_business_rule_violation(event: dict) -> dict:
    """Keep the event structurally valid but semantically wrong."""
    event_type = event["event_type"]
    if event_type == "order_created":
        event["total_amount"] = round(-abs(event["total_amount"] or 100), 2)
    elif event_type == "payment_completed":
        event["status"] = "unknown"
    elif event_type in ("product_viewed", "stock_updated"):
        event["stock_quantity"] = -abs(random.randint(1, 50))
    elif event_type == "product_returned":
        event["reason"] = "something_else"
        event["refund_amount"] = round(-abs(event["refund_amount"]), 2)
    return event


def apply_late_timestamp(event: dict) -> dict:
    """Push event_timestamp well behind ingestion_timestamp."""
    event_type = event["event_type"]
    delay_hours = random.uniform(48, 168) if event_type == "product_returned" else random.uniform(1, 6)
    original_ts = datetime.fromisoformat(event["event_timestamp"])
    event["event_timestamp"] = (original_ts - timedelta(hours=delay_hours)).isoformat()
    return event


def generate_event(recent_events: deque):
    """
    Returns (topic, event_dict, classification) for one generated event.
    classification is for local logging only - not part of the wire payload.
    """
    roll = random.random()

    # Duplicate: resend a real prior event verbatim, if we have one
    if roll < config.DUPLICATE_RATE and recent_events:
        original_topic, original_event = random.choice(recent_events)
        return original_topic, dict(original_event), "duplicate"

    # Schema violation
    threshold = config.DUPLICATE_RATE + config.SCHEMA_VIOLATION_RATE
    if roll < threshold:
        event_type = pick_event_type()
        event = finalize_amounts(BUILDERS[event_type]())
        event = apply_schema_violation(event)
        topic = config.EVENT_TYPE_TOPIC_MAP[event_type]
        return topic, event, "schema_violation"

    # Business rule violation
    threshold += config.BUSINESS_RULE_VIOLATION_RATE
    if roll < threshold:
        event_type = pick_event_type()
        event = finalize_amounts(BUILDERS[event_type]())
        event = apply_business_rule_violation(event)
        topic = config.EVENT_TYPE_TOPIC_MAP[event_type]
        return topic, event, "business_rule_violation"

    # Late event (event-type biased toward returns)
    threshold += config.LATE_EVENT_RATE
    if roll < threshold:
        event_type = pick_late_event_type()
        event = finalize_amounts(BUILDERS[event_type]())
        event = apply_late_timestamp(event)
        topic = config.EVENT_TYPE_TOPIC_MAP[event_type]
        return topic, event, "late"

    # Normal event
    event_type = pick_event_type()
    event = finalize_amounts(BUILDERS[event_type]())
    topic = config.EVENT_TYPE_TOPIC_MAP[event_type]
    return topic, event, "normal"


def get_partition_key(topic: str, event: dict) -> bytes:
    """order_id for order/payment/return topics, product_id for product topic."""
    if topic == "product-events":
        key = event.get("product_id", "")
    else:
        key = event.get("order_id", "")
    return str(key).encode("utf-8") if key else None


def main():
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k if k is None else k,
    )

    recent_events = deque(maxlen=config.RECENT_EVENTS_BUFFER_SIZE)
    interval = 1.0 / config.EVENTS_PER_SECOND
    counts = {"normal": 0, "duplicate": 0, "schema_violation": 0, "business_rule_violation": 0, "late": 0}

    print(f"Starting event generator: {config.EVENTS_PER_SECOND} events/sec -> {config.KAFKA_BOOTSTRAP_SERVERS}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            topic, event, classification = generate_event(recent_events)

            key = get_partition_key(topic, event)
            producer.send(topic, key=key, value=event)

            if classification == "normal":
                recent_events.append((topic, event))

            counts[classification] += 1
            total = sum(counts.values())
            if total % 20 == 0:
                print(f"[{total} sent] {counts}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopping. Final counts:")
        print(counts)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
