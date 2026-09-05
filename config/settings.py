"""
producer/config.py — locked producer behavior settings (Phase 1-2).

Change these values to tune throughput/fault rates; event_generator.py
should never hardcode these numbers directly.
"""

# --- Throughput ---
EVENTS_PER_SECOND = 5   # tune later to demo scaling: 5 -> 20 -> 100

# --- Event type mix (must sum to 1.0) ---
EVENT_TYPE_WEIGHTS = {
    "order_created": 0.25,
    "payment_completed": 0.25,
    "product_viewed": 0.35,
    "stock_updated": 0.05,
    "product_returned": 0.10,
}

# Maps each event_type to the Redpanda topic it belongs to
EVENT_TYPE_TOPIC_MAP = {
    "order_created": "order-events",
    "payment_completed": "payment-events",
    "product_viewed": "product-events",
    "stock_updated": "product-events",
    "product_returned": "return-events",
}

# --- Fault injection rates (mutually exclusive — sum stays well under 1.0, remainder is "normal") ---
DUPLICATE_RATE = 0.03
SCHEMA_VIOLATION_RATE = 0.025
BUSINESS_RULE_VIOLATION_RATE = 0.025
LATE_EVENT_RATE = 0.05
# remaining ~87% of events are normal, valid, on-time

# --- Late-event bias: when an event is chosen to be late, bias WHICH event type it is ---
# Returns are the realistic late-arriving event type; orders/payments are rarely late.
LATE_EVENT_TYPE_BIAS = {
    "order_created": 0.05,
    "payment_completed": 0.05,
    "product_viewed": 0.10,
    "stock_updated": 0.10,
    "product_returned": 0.70,
}

# --- Redpanda connection (host machine -> container via exposed port) ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# --- Regions used across events, matches the domain in the PRD ---
REGIONS = ["Maharashtra", "Karnataka", "Delhi", "Tamil Nadu", "Gujarat"]

# --- Recent-event buffer size (used to pick a real prior event to duplicate) ---
RECENT_EVENTS_BUFFER_SIZE = 50
