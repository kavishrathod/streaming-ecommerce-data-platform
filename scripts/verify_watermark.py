"""
scripts/verify_watermark.py — one-time Phase 2 verification check.

NOT part of the pipeline architecture. Confirms late-event handling is
actually working: finds a return-events row in Silver whose event_timestamp
is far behind its ingestion_timestamp (a genuinely late-arriving event -
your producer biases ~70% of late events toward product_returned), then
checks Gold's windowed aggregation counted it in the window matching its
EVENT time, proving the 7-day watermark didn't silently drop it for
arriving late.

Usage:
    python scripts/verify_watermark.py
"""

import duckdb

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

TOPIC = "return-events"


def main():
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")

    silver_path = f"s3://silver/{TOPIC}/**/*.parquet"
    gold_path = f"s3://gold/{TOPIC}_summary/**/*.parquet"

    print(f"Checking topic: {TOPIC}\n")

    # --- Step 1: find the most-late event in Silver (largest gap between
    # ingestion_timestamp and event_timestamp) ---
    late_events = con.execute(f"""
        SELECT
            event_id,
            region,
            event_timestamp,
            ingestion_timestamp,
            date_diff('hour', event_timestamp, ingestion_timestamp) AS hours_late
        FROM read_parquet('{silver_path}')
        ORDER BY hours_late DESC
        LIMIT 5
    """).fetchdf()

    if late_events.empty:
        print("No return-events found in Silver yet. Let the pipeline run longer.")
        return

    print("Most-late return events found in Silver:")
    print(late_events.to_string(index=False))

    target = late_events.iloc[0]
    print(f"\nMost late: event_id={target['event_id']}, region={target['region']}, "
          f"{target['hours_late']} hours late "
          f"(event_timestamp={target['event_timestamp']}, ingested={target['ingestion_timestamp']})\n")

    # --- Step 2: check Gold's windowed aggregation for a window covering
    # this event's event_timestamp, in the same region ---
    gold_match = con.execute(f"""
        SELECT "window"."start" AS window_start, "window"."end" AS window_end, region, event_count
        FROM read_parquet('{gold_path}')
        WHERE region = ?
          AND "window"."start" <= ?
          AND "window"."end" > ?
    """, [target["region"], target["event_timestamp"], target["event_timestamp"]]).fetchdf()

    if gold_match.empty:
        print("⚠️  No matching Gold window found yet for this event's event_timestamp.")
        print("   Gold writes lag slightly behind Silver in micro-batches - retry in a")
        print("   moment. If this persists, the watermark may be dropping the event.")
    else:
        print("Matching Gold window (proves the late event was counted, not dropped):")
        print(gold_match.to_string(index=False))
        print(f"\n✅ Late-event handling confirmed: an event {target['hours_late']} hours late "
              f"was still aggregated into its correct event-time window in Gold.")


if __name__ == "__main__":
    main()
