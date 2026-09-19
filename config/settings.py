"""
config/settings.py — centralized configuration (Phase 1-2).

All connection details (Kafka broker, MinIO credentials) load from
environment variables via a .env file, never hardcoded here. See
.env.example for the variables this expects. .env itself is gitignored -
create it locally by copying .env.example and filling in real values.

Producer behavior settings (throughput, fault rates) are NOT secrets and
stay as plain constants below, same as before.
"""

import os
from dotenv import load_dotenv

load_dotenv()  # reads .env in the project root, if present; no-op if missing

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
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- MinIO / S3 connection - credentials loaded from .env, never hardcoded ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", f"http://{MINIO_ENDPOINT}")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

# --- Regions used across events, matches the domain in the PRD ---
REGIONS = ["Maharashtra", "Karnataka", "Delhi", "Tamil Nadu", "Gujarat"]

# --- Recent-event buffer size (used to pick a real prior event to duplicate) ---
RECENT_EVENTS_BUFFER_SIZE = 50
