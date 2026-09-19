"""
scripts/verify_dedup.py — one-time Phase 2 verification check.

NOT part of the pipeline architecture. Confirms deduplication is actually
working by finding an event_id that appears more than once in Bronze
(raw, unfiltered) and checking it appears exactly once in Silver
(post-dedup).

Usage:
    python scripts/verify_dedup.py [topic]

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

    bronze_path = f"s3://bronze/{topic}/**/*.parquet"
    silver_path = f"s3://silver/{topic}/**/*.parquet"

    print(f"Checking topic: {topic}\n")

    # --- Step 1: find event_ids that appear more than once in Bronze ---
    # Bronze stores raw json_value (a string), not parsed columns, so we
    # extract event_id from the JSON text directly.
    dupes = con.execute(f"""
        SELECT
            json_extract_string(json_value, '$.event_id') AS event_id,
            COUNT(*) AS bronze_count
        FROM read_parquet('{bronze_path}')
        GROUP BY 1
        HAVING COUNT(*) > 1
        ORDER BY bronze_count DESC
        LIMIT 5
    """).fetchdf()

    if dupes.empty:
        print("No duplicate event_ids found in Bronze yet.")
        print("This can happen if the producer hasn't run long enough to hit its")
        print("~3% duplicate rate, or if Bronze doesn't have enough data yet.")
        print("Let the producer + Spark job run longer, then retry.")
        return

    print("Duplicate event_ids found in Bronze:")
    print(dupes.to_string(index=False))

    # --- Step 2: check the first duplicated event_id's count in Silver ---
    target_id = dupes.iloc[0]["event_id"]
    print(f"\nChecking event_id = {target_id} in Silver...\n")

    silver_count = con.execute(f"""
        SELECT COUNT(*) AS silver_count
        FROM read_parquet('{silver_path}')
        WHERE event_id = '{target_id}'
    """).fetchone()[0]

    bronze_count = int(dupes.iloc[0]["bronze_count"])
    print(f"Bronze count for this event_id: {bronze_count}")
    print(f"Silver count for this event_id: {silver_count}")

    if silver_count == 1:
        print("\n✅ Deduplication confirmed: appeared {} times in Bronze, exactly once in Silver.".format(bronze_count))
    elif silver_count == 0:
        print("\n⚠️  event_id not found in Silver at all - it may have been quarantined")
        print("   instead (check if it was also a schema/business-rule violation),")
        print("   or Silver hasn't caught up yet - Spark writes in micro-batches.")
    else:
        print(f"\n❌ Unexpected: found {silver_count} copies in Silver - dedup may not be working as expected.")


if __name__ == "__main__":
    main()
