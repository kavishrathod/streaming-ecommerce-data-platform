"""
order_events schema — Phase 1 (locked)

Represents order lifecycle events: order_created, order_cancelled.
event_id is the deduplication key.
event_timestamp vs ingestion_timestamp drives late-event/watermarking logic in Phase 2.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

ORDER_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),          # e.g. evt_a1b2c3d4 — dedup key
    StructField("event_type", StringType(), nullable=False),        # order_created | order_cancelled
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=False),
    StructField("region", StringType(), nullable=True),             # nullable on purpose — a natural quarantine trigger
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])

# Reference dict shape for the Python event producer (JSON payloads before Spark parses them)
ORDER_EVENT_EXAMPLE = {
    "event_id": "evt_a1b2c3d4",
    "event_type": "order_created",
    "order_id": "ord_00001234",
    "customer_id": "cust_00567",
    "product_id": "prod_00089",
    "quantity": 2,
    "unit_price": 499.00,
    "total_amount": 998.00,
    "region": "Maharashtra",
    "event_timestamp": "2026-09-05T10:30:00Z",
    "ingestion_timestamp": "2026-09-05T10:30:02Z",
}
