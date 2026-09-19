"""
data_quality/validators.py

Two distinct validation classes, matching the producer's fault design:

1. Schema validation  - required field is null/missing after JSON parsing,
   OR event_type itself is null/unrecognized (which happens when from_json
   fails entirely on malformed JSON, producing an all-null parsed struct -
   without this check such a row would match none of the per-type null-field
   checks below and would incorrectly pass through as "valid").
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

    IMPORTANT: only event types whose ENTIRE required-field set already
    exists as real columns in `df` are checked. Each topic is parsed with
    its own narrower schema (e.g. order-events has no payment_id column at
    all), so referencing another event type's fields would fail to resolve
    even inside an untaken F.when() branch - Spark still needs the column
    to exist at query-plan time regardless of whether that branch fires at
    runtime. product-events is the only topic where multiple event types
    (product_viewed, stock_updated) genuinely share one schema.

    A null or unrecognized event_type is ALWAYS a schema violation.
    """
    available_columns = set(df.columns)
    applicable_types = {
        et: fields for et, fields in REQUIRED_FIELDS.items()
        if set(fields).issubset(available_columns)
    }
    known_types = list(applicable_types.keys())
    unknown_type = ~F.col("event_type").isin(*known_types)

    is_null_any = unknown_type
    reason_parts = [F.when(unknown_type, F.lit("unknown_or_missing_event_type"))]
    for event_type, required in applicable_types.items():
        for f in required:
            applies_and_null = (F.col("event_type") == event_type) & F.col(f).isNull()
            is_null_any = is_null_any | applies_and_null
            reason_parts.append(F.when(applies_and_null, F.lit(f)))

    reason_array = F.array_remove(F.array(*[F.coalesce(p, F.lit("")) for p in reason_parts]), "")
    reason = F.concat(F.lit("null field(s)/invalid type: "), F.concat_ws(", ", reason_array))

    return df.withColumn("_schema_ok", ~is_null_any).withColumn(
        "_schema_reason", F.when(is_null_any, reason).otherwise(F.lit(None))
    )


def add_business_rule_check(df: DataFrame) -> DataFrame:
    """
    Adds `_business_ok` (bool) and `_business_reason` (string or null).
    Rule applied is chosen per-row based on the row's own event_type.
    product_viewed has NO stock check - stock is not a meaningful concept
    for a view event; only stock_updated checks stock_quantity.

    Same column-existence guard as add_schema_check: a rule referencing a
    field (status, stock_quantity, refund_amount, reason) that doesn't
    exist in this topic's schema is skipped entirely for that event type,
    rather than being wrapped in an F.when() branch that would still fail
    to resolve at query-plan time.
    """
    available_columns = set(df.columns)
    et = F.col("event_type")

    rule_defs = [
        ("order_created", ["total_amount"], F.col("total_amount") < 0 if "total_amount" in available_columns else None,
         "negative total_amount"),
        ("payment_completed", ["status"], ~F.col("status").isin("success", "failed") if "status" in available_columns else None,
         "invalid payment status"),
        ("product_viewed", [], F.lit(False), None),
        ("stock_updated", ["stock_quantity"], F.col("stock_quantity") < 0 if "stock_quantity" in available_columns else None,
         "negative stock_quantity"),
        (
            "product_returned",
            ["refund_amount", "reason"],
            (
                (F.col("refund_amount") < 0) | (~F.col("reason").isin("defective", "wrong_item", "not_needed", "other"))
                if {"refund_amount", "reason"}.issubset(available_columns)
                else None
            ),
            "negative refund_amount or invalid reason",
        ),
    ]

    bad = F.lit(False)
    reason = F.lit(None)
    for event_type, required_cols, condition, reason_text in rule_defs:
        if condition is None or not set(required_cols).issubset(available_columns):
            continue  # this event type's rule needs a column this topic doesn't have - skip it entirely
        applies = et == event_type
        bad = F.when(applies, condition).otherwise(bad)
        reason = F.when(applies & condition, F.lit(reason_text)).otherwise(reason)

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
