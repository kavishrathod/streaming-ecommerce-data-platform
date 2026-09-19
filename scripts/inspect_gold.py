"""
scripts/inspect_gold.py — quick ad-hoc inspection, not part of the pipeline.
Lists what windows/rows currently exist in Gold for a topic, so we can tell
whether Gold has no data at all yet vs. is just missing one specific window.
"""
import duckdb, sys

import sys as _sys, os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from config import settings as _config

MINIO_ENDPOINT = _config.MINIO_ENDPOINT
MINIO_ACCESS_KEY = _config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = _config.MINIO_SECRET_KEY

topic = sys.argv[1] if len(sys.argv) > 1 else "return-events"

con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")
con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")

gold_path = f"s3://gold/{topic}_summary/**/*.parquet"

result = con.execute(f"""
    SELECT "window"."start" AS window_start, "window"."end" AS window_end, region, event_count
    FROM read_parquet('{gold_path}')
    ORDER BY window_start
""").fetchdf()

print(f"Gold rows for {topic}_summary: {len(result)}")
if not result.empty:
    print(result.to_string(index=False))
    print(f"\nEarliest window: {result['window_start'].min()}")
    print(f"Latest window:   {result['window_start'].max()}")
else:
    print("Gold is completely empty for this topic.")
