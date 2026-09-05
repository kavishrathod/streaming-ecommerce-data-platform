"""
return_events schema — Phase 1 (locked)

Represents product return events: product_returned.
Links back to both order_events (order_id) and product_events (product_id).
Naturally demonstrates late-arriving events — returns realistically happen
days after the original order, so event_timestamp vs ingestion_timestamp
will show a real gap, not just a simulated one.
event_id is the deduplication key.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

RETURN_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),          # e.g. evt_r1a2b3c4 — dedup key
    StructField("event_type", StringType(), nullable=False),        # product_returned
    StructField("order_id", StringType(), nullable=False),          # links back to order_events
    StructField("product_id", StringType(), nullable=False),        # links back to product_events
    StructField("customer_id", StringType(), nullable=False),
    StructField("reason", StringType(), nullable=True),             # defective | wrong_item | not_needed | other
    StructField("refund_amount", DoubleType(), nullable=False),
    StructField("region", StringType(), nullable=True),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])

RETURN_EVENT_EXAMPLE = {
    "event_id": "evt_r1a2b3c4",
    "event_type": "product_returned",
    "order_id": "ord_00001234",
    "product_id": "prod_00089",
    "customer_id": "cust_00567",
    "reason": "defective",
    "refund_amount": 499.00,
    "region": "Maharashtra",
    "event_timestamp": "2026-09-05T10:30:00Z",
    "ingestion_timestamp": "2026-09-10T14:12:00Z",
}
