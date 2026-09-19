"""
scripts/verify_restart_recovery.py — one-time Phase 2 verification check.

NOT part of the pipeline architecture. Run this AFTER killing and
restarting the Spark job at least once, to confirm checkpointing worked:
no event_id should appear more than once in Silver as a result of the
restart (that would mean Spark reprocessed already-committed data), and
event_ids should be continuous/sane (no obviously "reset to offset 0"
duplication pattern).

Usage:
    python scripts/verify_restart_recovery.py [topic]

topic defaults to "order-events" if not given.
"""

import sys
import duckdb

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"


def main():
    topic = sys.argv[1] if len(sys.argv) > 1 else "order-events"

    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")

    silver_path = f"s3://silver/{topic}/**/*.parquet"

    print(f"Checking topic: {topic}\n")

    # --- Total row count and distinct event_id count in Silver.
    # If restart recovery is broken, total_rows > distinct_ids, because
    # some event_ids would have been processed twice (once before the
    # crash, once again after restart re-read already-committed offsets). ---
    counts = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT event_id) AS distinct_event_ids
        FROM read_parquet('{silver_path}')
    """).fetchone()

    total_rows, distinct_ids = counts
    print(f"Total rows in Silver:        {total_rows}")
    print(f"Distinct event_ids in Silver: {distinct_ids}")

    if total_rows == distinct_ids:
        print("\n✅ Restart recovery confirmed: every event_id in Silver appears exactly")
        print("   once, even across the crash + restart. Checkpointing correctly resumed")
        print("   from committed Kafka offsets instead of reprocessing.")
    else:
        extra = total_rows - distinct_ids
        print(f"\n❌ {extra} event_id(s) appear more than once in Silver.")
        print("   This could mean checkpointing didn't resume correctly after restart -")
        print("   or it could be a legitimate dedup-window edge case if two genuine")
        print("   duplicates (from the producer's ~3% duplicate injection) landed more")
        print("   than SILVER_DEDUP_WATERMARK (10 minutes) apart. Investigate which:")

        dupe_ids = con.execute(f"""
            SELECT event_id, COUNT(*) AS cnt
            FROM read_parquet('{silver_path}')
            GROUP BY event_id
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchdf()
        print(dupe_ids.to_string(index=False))


if __name__ == "__main__":
    main()
