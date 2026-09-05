"""
data_quality/quarantine.py

Invalid records are never dropped silently - they land in the `quarantine`
bucket with their DQ status/reason attached, partitioned by DQ status and
ingestion date, so they're browsable and debuggable later (e.g. via DuckDB).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def write_quarantine(df: DataFrame, event_type: str, quarantine_path: str, checkpoint_path: str):
    """
    df must already have `_dq_status` != "valid" rows only, and `_dq_reason`.
    Writes as a streaming sink in append mode, partitioned by dq status and
    ingestion date (derived from kafka_timestamp, since that's always present
    regardless of whether the payload itself parsed correctly).
    """
    quarantine_df = df.withColumn(
        "_quarantine_date", F.to_date(F.col("kafka_timestamp"))
    )

    query = (
        quarantine_df.writeStream.format("parquet")
        .option("path", f"{quarantine_path}/{event_type}")
        .option("checkpointLocation", f"{checkpoint_path}/{event_type}_quarantine")
        .outputMode("append")
        .partitionBy("_dq_status", "_quarantine_date")
        .start()
    )
    return query
