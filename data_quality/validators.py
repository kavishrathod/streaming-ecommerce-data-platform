"""
data_quality/validators.py

Two distinct validation classes, matching the producer's fault design:

1. Schema validation  - required field is null/missing after JSON parsing.
2. Business-rule validation - field is present and correctly typed, but the
   value violates a domain rule (negative amount, unknown status, etc).

Checks are driven by each row's own `event_type` column (via F.when chains),
not by a single event_type passed in for the whole call - this lets one
DataFrame safely mix multiple event types, as the shared `product-events`
topic does (product_viewed + stock_updated).

Both operate on a parsed Spark DataFrame (one row per event, already cast
to the locked StructType schema) and return a DataFrame with an added
`_dq_status` ("valid" | "schema_violation" | "business_rule_violation")
and `_dq_reason` column explaining the failure.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_FIELDS = {
    "order_created": ["order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount"],
    "payment_completed": ["order_id", "payment_id", "amount", "payment_method", "status"],
    "product_viewed": ["product_id", "product_name", "price"],
    "stock_updated": ["product_id", "product_name", "price", "stock_quantity"],
    "product_returned": ["order_id", "product_id", "customer_id", "refund_amount"],
}


def add_schema_check(df: DataFrame) -> DataFrame:
    """
    Adds `_schema_ok` (bool) and `_schema_reason` (string or null).
    Required fields are looked up per-row from the row's own event_type,
    via a when/otherwise chain over REQUIRED_FIELDS - so a single call
    handles a DataFrame containing multiple event types correctly.
    """
    all_fields = sorted({f for fields in REQUIRED_FIELDS.values() for f in fields})

    is_null_any = F.lit(False)
    reason_parts = []
    for event_type, required in REQUIRED_FIELDS.items():
        for f in required:
            applies_and_null = (F.col("event_type") == event_type) & F.col(f).isNull()
            is_null_any = is_null_any | applies_and_null
            reason_parts.append(F.when(applies_and_null, F.lit(f)))

    reason_array = F.array_remove(F.array(*[F.coalesce(p, F.lit("")) for p in reason_parts]), "")
    reason = F.concat(F.lit("null field(s): "), F.concat_ws(", ", reason_array))

    return df.withColumn("_schema_ok", ~is_null_any).withColumn(
        "_schema_reason", F.when(is_null_any, reason).otherwise(F.lit(None))
    )


def add_business_rule_check(df: DataFrame) -> DataFrame:
    """
    Adds `_business_ok` (bool) and `_business_reason` (string or null).
    Rule applied is chosen per-row based on the row's own event_type.
    """
    et = F.col("event_type")

    bad = (
        F.when(et == "order_created", F.col("total_amount") < 0)
        .when(et == "payment_completed", ~F.col("status").isin("success", "failed"))
        .when(et == "product_viewed", F.lit(False))  # no stock check for views - stock is not meaningful here
        .when(et == "stock_updated", F.col("stock_quantity") < 0)
        .when(
            et == "product_returned",
            (F.col("refund_amount") < 0) | (~F.col("reason").isin("defective", "wrong_item", "not_needed", "other")),
        )
        .otherwise(F.lit(False))
    )

    reason = (
        F.when(et == "order_created", F.lit("negative total_amount"))
        .when(et == "payment_completed", F.lit("invalid payment status"))
        .when(et == "stock_updated", F.lit("negative stock_quantity"))
        .when(et == "product_returned", F.lit("negative refund_amount or invalid reason"))
        .otherwise(F.lit(None))
    )

    return df.withColumn("_business_ok", ~bad).withColumn(
        "_business_reason", F.when(bad, reason).otherwise(F.lit(None))
    )


def apply_dq_checks(df: DataFrame) -> DataFrame:
    """
    Runs both checks and collapses them into a single `_dq_status` +
    `_dq_reason` pair. Schema violations take priority over business-rule
    violations if somehow both fire on the same row. Works on a DataFrame
    containing a single event type OR a mix (e.g. the product-events topic).
    """
    df = add_schema_check(df)
    df = add_business_rule_check(df)

    status = (
        F.when(~F.col("_schema_ok"), F.lit("schema_violation"))
        .when(~F.col("_business_ok"), F.lit("business_rule_violation"))
        .otherwise(F.lit("valid"))
    )
    reason = F.coalesce(F.col("_schema_reason"), F.col("_business_reason"))

    return (
        df.withColumn("_dq_status", status)
        .withColumn("_dq_reason", reason)
        .drop("_schema_ok", "_schema_reason", "_business_ok", "_business_reason")
    )
