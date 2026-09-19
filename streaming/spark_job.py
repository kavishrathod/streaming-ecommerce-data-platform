"""
streaming/spark_job.py — Phase 2 flagship deliverable.

For each of the four event topics, this job:
  1. Reads from Redpanda (Kafka-compatible) as a stream.
  2. Writes the untouched raw JSON + Kafka metadata to Bronze BEFORE parsing,
     so a schema-violating or type-corrupted record still has its original
     bytes preserved there.
  3. Parses JSON against the locked StructType schema.
  4. Runs schema + business-rule DQ checks (data_quality/validators.py),
     driven by each row's own event_type column - handles a mixed-type
     topic (product-events: product_viewed + stock_updated) in one pass.
     A null/unrecognized event_type is always a schema violation.
  5. Routes invalid records to the `quarantine` bucket - never dropped
     silently, and the original json_value is preserved there too.
  6. Valid records go down two INDEPENDENT branches, each with its own
     single watermark (deliberately not chained/shared - see note below):
       - Silver branch: dedup with a tight 10-minute watermark.
       - Gold branch: dedup with a generous 7-day watermark, then a small
         windowed aggregation - this is where late-arriving events
         (especially product_returned) are visibly still counted.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
        org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        streaming/spark_job.py
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.schemas import (
    ORDER_EVENT_SCHEMA,
    PAYMENT_EVENT_SCHEMA,
    PRODUCT_EVENT_SCHEMA,
    RETURN_EVENT_SCHEMA,
)
from data_quality.validators import apply_dq_checks
from data_quality.quarantine import write_quarantine

# --- MinIO / S3 and Redpanda connection, centralized in config/settings.py
# (loaded from .env - never hardcoded here) ---
from config import settings as config

MINIO_ENDPOINT = config.MINIO_ENDPOINT_URL
MINIO_ACCESS_KEY = config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = config.MINIO_SECRET_KEY
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS

# --- Watermarks: Silver and Gold are independent branches (separate
# writeStream().start() calls), each getting its own single withWatermark
# call. This sidesteps any ambiguity about Spark's multipleWatermarkPolicy
# entirely - they never share a chained lineage, so there's no question of
# one watermark silently overriding the other. ---
SILVER_DEDUP_WATERMARK = "10 minutes"   # tight - bounds Silver's dedup state store
GOLD_WATERMARK = "7 days"               # generous - lets late product_returned events still be aggregated

TOPICS = {
    "order-events": ORDER_EVENT_SCHEMA,
    "payment-events": PAYMENT_EVENT_SCHEMA,
    "product-events": PRODUCT_EVENT_SCHEMA,  # covers product_viewed + stock_updated
    "return-events": RETURN_EVENT_SCHEMA,
}

# Gold groups by a dimension that actually exists in each topic's schema.
# order/return events have `region`; payment events don't - group by
# payment_method instead; product events don't have region either - group
# by category instead. Using `region` everywhere would crash on topics
# whose schema doesn't have that column.
GOLD_DIMENSION_BY_TOPIC = {
    "order-events": "region",
    "payment-events": "payment_method",
    "product-events": "category",
    "return-events": "region",
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("streaming-ecommerce-platform")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")  # keep it light for an 8GB machine
        .getOrCreate()
    )


def read_topic_stream(spark: SparkSession, topic: str):
    """Raw stream: untouched JSON + Kafka metadata, for Bronze. No parsing here."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    raw_with_meta = raw.selectExpr(
        "CAST(value AS STRING) as json_value",
        "CAST(key AS STRING) as kafka_key",
        "topic as kafka_topic",
        "partition as kafka_partition",
        "offset as kafka_offset",
        "timestamp as kafka_timestamp",
    )
    return raw_with_meta


def parse_events(raw_with_meta, schema):
    """
    Parses json_value against the schema. Called AFTER Bronze has already
    persisted the untouched raw payload, so parse failures never lose data.
    json_value is kept in the output (not dropped) so quarantine can still
    show the original payload for invalid rows; Silver drops it explicitly
    later since it's not needed for clean, validated data.
    """
    parsed = (
        raw_with_meta.withColumn("data", F.from_json(F.col("json_value"), schema))
        .select(
            "data.*",
            "json_value",
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    )
    return parsed


def run_pipeline_for_topic(spark: SparkSession, topic: str, schema):
    """
    Sets up the full Bronze -> DQ split -> Silver -> Gold chain for one topic.
    Returns the list of active StreamingQuery objects so main() can await them.
    """
    raw_with_meta = read_topic_stream(spark, topic)
    queries = []

    # --- Bronze: the untouched raw JSON payload + Kafka metadata, exactly as
    # received, written BEFORE parsing. ---
    bronze_query = (
        raw_with_meta.writeStream.format("parquet")
        .option("path", f"s3a://bronze/{topic}")
        .option("checkpointLocation", f"s3a://bronze/_checkpoints/{topic}")
        .outputMode("append")
        .start()
    )
    queries.append(bronze_query)

    # --- Parse now, downstream of Bronze ---
    parsed = parse_events(raw_with_meta, schema)

    # --- DQ check. Driven by each row's own event_type column, so a topic
    # carrying multiple event types (product-events) is handled correctly
    # in a single pass. Null/unknown event_type is always a violation. ---
    checked = apply_dq_checks(parsed)

    invalid = checked.filter(F.col("_dq_status") != "valid")
    valid = checked.filter(F.col("_dq_status") == "valid").drop("_dq_status", "_dq_reason")

    # --- Quarantine: invalid records, never dropped silently. json_value
    # (the original payload) is still present here - not yet dropped. ---
    quarantine_query = write_quarantine(
        invalid, topic, quarantine_path="s3a://quarantine", checkpoint_path="s3a://quarantine/_checkpoints"
    )
    queries.append(quarantine_query)

    # --- Silver branch: independent from Gold. Own watermark, own dedup,
    # own state store bounded to 10 minutes. json_value dropped here - not
    # needed once data has passed DQ and is clean. ---
    silver_deduped = (
        valid.drop("json_value")
        .withWatermark("event_timestamp", SILVER_DEDUP_WATERMARK)
        .dropDuplicates(["event_id"])
    )

    silver_query = (
        silver_deduped.writeStream.format("parquet")
        .option("path", f"s3a://silver/{topic}")
        .option("checkpointLocation", f"s3a://silver/_checkpoints/{topic}")
        .outputMode("append")
        .start()
    )
    queries.append(silver_query)

    # --- Gold branch: independent from Silver. Own watermark, generous
    # enough that a product_returned event arriving days late is still
    # picked up by the windowed aggregation - this is the visible proof of
    # late-event handling the project is meant to demonstrate. ---
    gold_dimension = GOLD_DIMENSION_BY_TOPIC[topic]
    gold_deduped = (
        valid.drop("json_value")
        .withWatermark("event_timestamp", GOLD_WATERMARK)
        .dropDuplicates(["event_id"])
    )

    gold_agg = gold_deduped.groupBy(
        F.window("event_timestamp", "5 minutes"),
        F.coalesce(F.col(gold_dimension), F.lit("unknown")).alias(gold_dimension),
    ).agg(F.count("*").alias("event_count"))

    gold_query = (
        gold_agg.writeStream.format("parquet")
        .option("path", f"s3a://gold/{topic}_summary")
        .option("checkpointLocation", f"s3a://gold/_checkpoints/{topic}_summary")
        .outputMode("append")
        .start()
    )
    queries.append(gold_query)

    return queries


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    all_queries = []
    for topic, schema in TOPICS.items():
        print(f"Starting pipeline for topic: {topic}")
        queries = run_pipeline_for_topic(spark, topic, schema)
        all_queries.extend(queries)

    print(f"\n{len(all_queries)} streaming queries running. Waiting...\n")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
