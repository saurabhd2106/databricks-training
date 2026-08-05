"""Shared helpers for building event-bus bronze-like DataFrames in unit tests."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType


RAW_VALUE_SCHEMA = StructType(
    [
        StructField("_raw_value", StringType(), True),
        StructField("_topic", StringType(), True),
        StructField("_partition", LongType(), True),
        StructField("_offset", LongType(), True),
        StructField("_kafka_timestamp", TimestampType(), True),
        StructField("_ingest_ts", TimestampType(), True),
    ]
)


def raw_value_df(spark: SparkSession, payloads: list[str | None]) -> DataFrame:
    """Build a DataFrame of Kafka-like `_raw_value` strings for parse tests."""
    rows = [
        {
            "_raw_value": payload,
            "_topic": "actuarial.claims",
            "_partition": 0,
            "_offset": i,
            "_kafka_timestamp": datetime(2025, 1, 1, 0, 0, 0),
            "_ingest_ts": datetime(2025, 1, 1, 0, 0, 0),
        }
        for i, payload in enumerate(payloads)
    ]
    return spark.createDataFrame(rows, schema=RAW_VALUE_SCHEMA)


def claim_json(**overrides: Any) -> str:
    base = {
        "claim_id": "CLM-001",
        "policy_id": "POL-001",
        "event_id": "CYC-001",
        "date_of_loss": "2025-03-10",
        "reported_date": "2025-03-15",
        "peril_type": "Wind",
        "claim_status": "Open",
        "incurred_amount": 10000.0,
        "paid_to_date": 2500.0,
        "snapshot_date": "2025-03-15",
    }
    base.update(overrides)
    return json.dumps(base)
