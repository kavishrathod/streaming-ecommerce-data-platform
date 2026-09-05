"""
verify_duckdb_minio.py — one-time Phase 1 infrastructure smoke test.

NOT part of the pipeline architecture. Run this once to prove the
MinIO -> Parquet -> DuckDB read path works before building the real
producer/streaming code. Safe to delete after Phase 1 is confirmed,
or keep as a debugging tool if MinIO/DuckDB connectivity ever breaks later.

Usage:
    python scripts/verify_duckdb_minio.py

Requires only the `minio` and `minio-init` services running:
    docker compose up -d minio minio-init
"""

import duckdb
import pandas as pd
from datetime import datetime, timezone

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "gold"
TEST_KEY = "smoke_test/sample.parquet"


def main():
    con = duckdb.connect()

    # Load the extensions DuckDB needs to talk to S3-compatible storage
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Point DuckDB's S3 client at the local MinIO instance instead of AWS
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")   # required for MinIO (vs AWS's virtual-hosted style)
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")

    # --- Step 1: create a tiny sample dataset shaped like a Gold-layer output ---
    sample = pd.DataFrame({
        "product_id": ["prod_00089", "prod_00090", "prod_00091"],
        "product_name": ["Wireless Mouse", "USB-C Cable", "Laptop Stand"],
        "total_sales": [4990.0, 1200.0, 2499.0],
        "region": ["Maharashtra", "Karnataka", "Maharashtra"],
        "as_of": [datetime.now(timezone.utc)] * 3,
    })

    local_path = "sample.parquet"
    sample.to_parquet(local_path, index=False)
    print(f"[1/3] Wrote local sample Parquet file: {local_path}")

    # --- Step 2: upload it to MinIO's gold bucket via DuckDB's COPY, using the local file ---
    con.register("sample_df", sample)
    s3_path = f"s3://{BUCKET}/{TEST_KEY}"
    con.execute(f"COPY sample_df TO '{s3_path}' (FORMAT PARQUET);")
    print(f"[2/3] Uploaded sample data to MinIO: {s3_path}")

    # --- Step 3: read it straight back from MinIO, proving the query path works ---
    result = con.execute(f"SELECT * FROM read_parquet('{s3_path}') ORDER BY total_sales DESC;").fetchdf()
    print("[3/3] Read back from MinIO via DuckDB:\n")
    print(result.to_string(index=False))

    print("\n✅ MinIO -> Parquet -> DuckDB path confirmed working.")


if __name__ == "__main__":
    main()
