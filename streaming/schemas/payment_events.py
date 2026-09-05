"""
payment_events schema — Phase 1 (locked)

Represents payment lifecycle events: payment_completed, payment_failed.
Links back to order_events via order_id.
event_id is the deduplication key.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

PAYMENT_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),          # e.g. evt_p1a2b3c4 — dedup key
    StructField("event_type", StringType(), nullable=False),        # payment_completed | payment_failed
    StructField("order_id", StringType(), nullable=False),          # links back to order_events
    StructField("payment_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("payment_method", StringType(), nullable=False),    # UPI | Card | NetBanking | COD
    StructField("status", StringType(), nullable=False),            # success | failed
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])

PAYMENT_EVENT_EXAMPLE = {
    "event_id": "evt_p1a2b3c4",
    "event_type": "payment_completed",
    "order_id": "ord_00001234",
    "payment_id": "pay_00009988",
    "amount": 998.00,
    "payment_method": "UPI",
    "status": "success",
    "event_timestamp": "2026-09-05T10:30:15Z",
    "ingestion_timestamp": "2026-09-05T10:30:17Z",
}
