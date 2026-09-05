"""
streaming/spark_job.py — Phase 2 flagship deliverable.

For each of the four event topics, this job:
  1. Reads from Redpanda (Kafka-compatible) as a stream.
  2. Parses JSON against the locked StructType schema.
  3. Runs schema + business-rule DQ checks (data_quality/validators.py).
  4. Routes invalid records to the `quarantine` bucket - never dropped silently.
  5. Deduplicates valid records by event_id within a watermark window.
  6. Writes valid, deduplicated records to Bronze (raw-ish, post-parse) and
     Silver (deduplicated, DQ-passed) as Parquet on MinIO.
  7. Runs a small windowed aggregation into Gold (event counts / amounts per
     region per window) - this is also where watermarking/late-event
     handling is visibly demonstrated.

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

# --- MinIO / S3 connection ---
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

# --- Redpanda connection ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# --- Watermark: how late an event can arrive and still be processed for
# windowed aggregation. Returns can be legitimately days late, but for a
# first version we cap the watermark generously rather than unbounded. ---
WATERMARK_DELAY = "7 days"
DEDUP_WATERMARK_DELAY = "10 minutes"   # separate, tighter window just for de-dup

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


def read_topic_stream(spark: SparkSession, topic: str, schema):
    """
    Returns the raw stream (untouched JSON + Kafka metadata, for Bronze)
    and does NOT parse here - parsing happens separately so a malformed
    or type-corrupted payload's original bytes are still preserved in Bronze.
    """
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
    """Parses json_value against the schema. Called AFTER Bronze has already
    persisted the untouched raw payload, so parse failures never lose data."""
    parsed = (
        raw_with_meta.withColumn("data", F.from_json(F.col("json_value"), schema))
        .select("data.*", "kafka_timestamp", "kafka_key", "kafka_partition", "kafka_offset")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    )
    return parsed


def run_pipeline_for_topic(spark: SparkSession, topic: str, schema):
    """
    Sets up the full Bronze -> DQ split -> Silver -> Gold chain for one topic.
    Returns the list of active StreamingQuery objects so main() can await them.
    """
    raw_with_meta = read_topic_stream(spark, topic, schema)
    queries = []

    # --- Bronze: the untouched raw JSON payload + Kafka metadata, exactly as
    # received. This is written BEFORE parsing, so a schema-violating or
    # type-corrupted record still has its original bytes preserved here. ---
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
    # carrying multiple event types (product-events: product_viewed +
    # stock_updated) is handled correctly in a single pass - no cross-frame
    # merging needed. ---
    checked = apply_dq_checks(parsed)

    invalid = checked.filter(F.col("_dq_status") != "valid")
    valid = checked.filter(F.col("_dq_status") == "valid").drop("_dq_status", "_dq_reason")

    # --- Quarantine: invalid records, never dropped silently ---
    quarantine_query = write_quarantine(
        invalid, topic, quarantine_path="s3a://quarantine", checkpoint_path="s3a://quarantine/_checkpoints"
    )
    queries.append(quarantine_query)

    # --- Deduplication: drop repeat event_id within a watermark window ---
    deduped = valid.withWatermark("event_timestamp", DEDUP_WATERMARK_DELAY).dropDuplicates(
        ["event_id"]
    )

    # --- Silver: cleaned, deduplicated, DQ-passed events ---
    silver_query = (
        deduped.writeStream.format("parquet")
        .option("path", f"s3a://silver/{topic}")
        .option("checkpointLocation", f"s3a://silver/_checkpoints/{topic}")
        .outputMode("append")
        .start()
    )
    queries.append(silver_query)

    # --- Gold: windowed aggregation. Dimension differs per topic, since not
    # every event schema has a `region` field - grouping by a column that
    # doesn't exist would crash the job, not just misbehave. ---
    gold_dimension = GOLD_DIMENSION_BY_TOPIC[topic]
    gold_agg = (
        deduped.withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            F.coalesce(F.col(gold_dimension), F.lit("unknown")).alias(gold_dimension),
        )
        .agg(F.count("*").alias("event_count"))
    )

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
