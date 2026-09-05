"""
product_events schema — Phase 1 (locked)

Represents catalog/inventory-side events: product_viewed, stock_updated.
Not tied to a single order — feeds Gold-layer aggregates like top-products,
low-stock, category performance.
event_id is the deduplication key.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

PRODUCT_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),          # e.g. evt_pr1a2b3c — dedup key
    StructField("event_type", StringType(), nullable=False),        # product_viewed | stock_updated
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=True),
    StructField("stock_quantity", IntegerType(), nullable=True),    # relevant for stock_updated events
    StructField("price", DoubleType(), nullable=False),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])

PRODUCT_EVENT_EXAMPLE = {
    "event_id": "evt_pr1a2b3c",
    "event_type": "product_viewed",
    "product_id": "prod_00089",
    "product_name": "Wireless Mouse",
    "category": "Electronics",
    "stock_quantity": 145,
    "price": 499.00,
    "event_timestamp": "2026-09-05T10:29:40Z",
    "ingestion_timestamp": "2026-09-05T10:29:42Z",
}
