"""Quarantine helpers for Event Hubs bronze raw streams."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def parse_or_null_key(key_col: str) -> Column:
    """Rows that fail bronze ingest quality (parse error or null business key)."""
    return F.col("_parse_error").isNotNull() | F.col(key_col).isNull()


def quarantine_reason(key_col: str) -> Column:
    """Human-readable reason string for quarantine rows."""
    parse_err = F.when(F.col("_parse_error").isNotNull(), F.col("_parse_error"))
    null_key = F.when(F.col(key_col).isNull(), F.lit(f"{key_col}_null"))
    return F.concat_ws(",", parse_err, null_key)


def quarantine_from_raw(raw: DataFrame, key_col: str) -> DataFrame:
    """Filter invalid rows from a bronze raw stream and stamp quarantine metadata."""
    return (
        raw.filter(parse_or_null_key(key_col))
        .withColumn("quarantine_reason", quarantine_reason(key_col))
        .withColumn("_quarantine_ts", F.current_timestamp())
    )
