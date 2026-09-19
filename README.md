# Real-Time Streaming E-Commerce Data Platform

A local, fully open-source streaming data pipeline that ingests simulated e-commerce events, validates and deduplicates them in real time, and lands them in a Bronze/Silver/Gold data lake — built to demonstrate production-style streaming data engineering, not a toy batch pipeline.

**Cost: $0.** Everything runs locally via Docker — Redpanda (Kafka-compatible), Spark Structured Streaming, MinIO (S3-compatible storage), and DuckDB.

---

## Architecture

```
Python Event Generator
        │
        ▼
     Redpanda  (order-events, payment-events, product-events, return-events)
        │
        ▼
Spark Structured Streaming
        │
        ├──► Bronze (raw JSON + Kafka metadata, written before parsing)
        │
        ▼
   Parse + Schema/Business-Rule Validation
        │
   ┌────┴────┐
   ▼         ▼
Invalid    Valid
   │         │
   ▼         ├──► Silver dedup (10-min watermark) ──► Silver (Parquet, MinIO)
Quarantine   │
(Parquet,    └──► Gold dedup (7-day watermark) ──► Gold windowed aggregates
 MinIO)                                              (Parquet, MinIO)

DuckDB reads Gold Parquet directly off MinIO — no separate load step.
```

**Why two separate watermarks for Silver and Gold:** Spark computes one global watermark per event-time column *within a single query plan*. Silver and Gold are independent streaming queries (separate `writeStream().start()` calls), so each gets its own watermark safely — a tight 10-minute one bounds Silver's dedup state store, while a generous 7-day one on Gold ensures a `product_returned` event arriving days late still gets aggregated instead of dropped.

---

## Data Flow

The lifecycle of one event, end to end:

1. The Python producer generates simulated e-commerce events.
2. Events are published to Redpanda topics using domain-specific partition keys (`order_id` / `product_id`).
3. Spark Structured Streaming consumes the topics.
4. Raw JSON and Kafka metadata are written to **Bronze** before any parsing happens.
5. Events are parsed against their event-specific schemas.
6. Schema and business-rule validation classify each event as valid or invalid.
7. Invalid events are preserved in **Quarantine** with the original payload and failure reason — never dropped silently.
8. Valid events go down two independent branches:
   - **Silver**: a tight 10-minute watermark bounds deduplication state; deduplicated events land as Parquet.
   - **Gold**: a generous 7-day watermark allows late-arriving events (especially `product_returned`) to still be aggregated into 5-minute windows.
9. DuckDB queries Gold Parquet directly off MinIO — no separate load step.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event producer | Python |
| Message broker | Redpanda (Kafka-compatible) |
| Stream processing | PySpark Structured Streaming |
| Object storage | MinIO (S3-compatible) |
| Storage format | Parquet |
| Analytics query | DuckDB (reads Gold Parquet directly) |
| Containerization | Docker Compose |

---

## What This Demonstrates

- **Streaming ingestion** — Kafka-style topics, partitioning by `order_id`/`product_id`, producer/consumer patterns
- **Schema validation** — required-field and type checks, driven per-row by event type
- **Business-rule validation** — semantic checks (negative amounts, invalid statuses) distinct from schema checks
- **Quarantine, not silent drop** — every invalid record is preserved with its original JSON payload and failure reason, partitioned by status and date
- **Deduplication** — event-level duplicate protection using `event_id`, verified by comparing Bronze and Silver records (a duplicate appearing 2x in Bronze collapses to 1x in Silver)
- **Watermarking / late-event handling** — Gold uses a 7-day event-time watermark so sufficiently late events, including delayed product_returned events within the configured watermark, can contribute to windowed aggregation.
- **Checkpointing / restart recovery** — restart behavior verified by killing the Spark job mid-run and restarting, then validating the resulting stream state (no duplicate `event_id`s attributable to the restart)
- **Bronze/Silver/Gold medallion architecture** — raw fidelity preserved, then progressively cleaned and aggregated

---

## Data Model

Four event types, each independently schema-locked:

- `order_created` — order placement events
- `payment_completed` — payment lifecycle events, linked via `order_id`
- `product_viewed` / `stock_updated` — catalog/inventory events (shared topic)
- `product_returned` — return events, linked via `order_id` and `product_id`; deliberately modeled as the most likely event type to arrive late.

## Fault Injection (Producer)

The event generator deliberately injects controlled "bad" data so the pipeline has something real to catch:

| Category | Rate | Purpose |
|---|---|---|
| Normal | ~87% | Baseline valid traffic |
| Duplicate | ~3% | Tests deduplication |
| Schema violation | ~2.5% | Null/missing required field, wrong type |
| Business-rule violation | ~2.5% | Negative amount, invalid status, etc. |
| Late event | ~5% | Tests watermarking (biased toward `product_returned`) |

---

## Running It

**Prerequisites:** Docker Desktop, Python 3.10+, Java 11, PySpark 3.5.3

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Start the event producer (separate terminal)
python -m producer.event_generator

# 3. Start the Spark pipeline (separate terminal)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 streaming/spark_job.py
```

MinIO Console: `http://localhost:9001` (minioadmin / minioadmin123)
Redpanda Console: `http://localhost:8080`

## Verification Scripts

One-off scripts (in `scripts/`) used to prove each pipeline guarantee, not part of the core architecture:

- `verify_duckdb_minio.py` — confirms the MinIO → Parquet → DuckDB read path
- `verify_dedup.py` — confirms duplicate `event_id`s collapse from Bronze to Silver
- `verify_watermark.py` — confirms late events survive into Silver rather than being dropped
- `verify_restart_recovery.py` — validates the resulting stream state after stopping and restarting the Spark job.
---

## Failure Handling

| Failure / Data Issue | Handling |
|---|---|
| Duplicate event | `event_id` deduplication (Silver branch) |
| Missing/invalid field | Quarantine, tagged `schema_violation` |
| Business-rule violation (negative amount, invalid status, etc.) | Quarantine, tagged `business_rule_violation` |
| Late-arriving event | Event-time watermark (7 days on the Gold branch) |
| Spark process crash/restart | Streaming checkpoints allow the query to resume from its recorded source progress |
| Malformed/corrupt raw payload | Preserved as-is in Bronze before parsing, so the original payload remains available for investigation even if parsing fails |
| Analytics query | DuckDB reads Gold Parquet directly — no separate load/ETL step to fail |

---

## Key Design Decisions

**Redpanda instead of direct producer → Spark communication**
A Kafka-compatible event buffer that decouples event production from stream processing and allows events to remain available while the consumer is temporarily unavailable.

**MinIO instead of cloud object storage**
S3-compatible local object storage, so the whole project runs free and reproducibly on any machine with Docker.

**Parquet instead of raw JSON for Silver/Gold**
Columnar format suited to analytical workloads and directly queryable by DuckDB.

**Bronze written before parsing**
Preserves the original event and Kafka metadata before downstream parsing, making malformed or type-corrupted records available for investigation.

**Separate Silver and Gold streaming queries**
Spark computes one global watermark per event-time column *within a single query plan*. Splitting Silver and Gold into independent queries lets each have its own watermark safely — a tight one for Silver's dedup state, a generous one for Gold's late-event tolerance — without one silently overriding the other.

**DuckDB instead of a separate analytical database**
Queries Gold Parquet directly off MinIO with no server to run and no separate load step.

---

## Production-Style vs. Local Scope

This project intentionally runs as a local production-style simulation — the patterns are real, the infrastructure is scaled down to run free on a laptop.

**Implemented:**
- Kafka-compatible streaming ingestion with topic partitioning
- Structured streaming with event-time processing
- Schema and business-rule data-quality validation
- Quarantine with original-payload preservation
- Deduplication, watermarking, checkpointing/restart recovery
- Bronze/Silver/Gold medallion architecture
- S3-compatible object storage, Parquet, direct analytical querying

**Local simplifications:**
- Single Redpanda broker (no multi-broker cluster/replication)
- Local MinIO instead of managed cloud object storage
- Simulated event producer instead of real transaction traffic
- Local Spark execution (no cluster manager / multi-node)
- No Kubernetes deployment, no cloud monitoring/alerting stack

The core data-flow design can be migrated to a managed/cloud environment with relatively limited architectural change — for example, Redpanda → Amazon MSK/Confluent, MinIO → Amazon S3, and local Spark → Amazon EMR/Databricks. The core streaming, validation, storage, and analytical processing patterns would remain the same, while infrastructure-specific configuration and operational concerns would change.

---

## Project Scope

This repository contains the completed core Data Engineering pipeline:
infrastructure, streaming ingestion, validation, quarantine,
deduplication, watermarking, checkpointing, and analytical querying.

Potential future extensions include local LLM-powered analytics,
Airflow-based batch workflows, and a Streamlit dashboard.
These are intentionally outside the current project scope.

---

## Screenshots

**Redpanda topics** — 4 topics, 3 partitions each, as designed:
![Redpanda Topics](screenshots/redpanda-topics.png)

**MinIO buckets** — Bronze/Silver/Gold/quarantine, populated with real data:
![MinIO Buckets](screenshots/minio-buckets.png)

**Silver detail** — actual Parquet part-files landing in `silver/order-events/`:
![MinIO Silver Detail](screenshots/minio-silver-detail.png)

**Spark pipeline running** — all 4 topics started, 16 streaming queries active:
![Spark Running](screenshots/spark-running.png)

**Producer fault injection** — live counts across all 5 categories (normal, duplicate, schema violation, business-rule violation, late):
![Producer Fault Injection](screenshots/producer-fault-injection.png)
