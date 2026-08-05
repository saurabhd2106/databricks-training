"""Shared helpers for building bronze-like DataFrames in unit tests."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def with_bronze_audit(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    schema: StructType | None = None,
) -> DataFrame:
    """Create a DataFrame and attach streaming bronze audit columns if missing."""
    df = spark.createDataFrame(rows, schema=schema) if schema else spark.createDataFrame(rows)
    cols = set(df.columns)
    if "_ingest_ts" not in cols:
        df = df.withColumn("_ingest_ts", F.lit(datetime(2025, 1, 1, 0, 0, 0)))
    if "_source_file" not in cols:
        df = df.withColumn("_source_file", F.lit("fixture.csv"))
    if "_rescued_data" not in cols:
        df = df.withColumn("_rescued_data", F.lit(None).cast("string"))
    return df


CLAIMS_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("date_of_loss", StringType(), True),
        StructField("reported_date", StringType(), True),
        StructField("peril_type", StringType(), True),
        StructField("claim_status", StringType(), True),
        StructField("incurred_amount", StringType(), True),
        StructField("paid_to_date", StringType(), True),
        StructField("snapshot_date", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingest_ts", TimestampType(), True),
        StructField("_rescued_data", StringType(), True),
    ]
)

EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingest_ts", TimestampType(), True),
        StructField("_rescued_data", StringType(), True),
    ]
)

PREMIUMS_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), True),
        StructField("insurer_name", StringType(), True),
        StructField("postcode", IntegerType(), True),
        StructField("region_name", StringType(), True),
        StructField("wind_risk_band", StringType(), True),
        StructField("building_type", StringType(), True),
        StructField("sum_insured", StringType(), True),
        StructField("mitigation_flag", StringType(), True),
        StructField("annual_premium", StringType(), True),
        StructField("policy_start_date", StringType(), True),
        StructField("policy_end_date", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingest_ts", TimestampType(), True),
        StructField("_rescued_data", StringType(), True),
    ]
)

RISK_ZONE_SCHEMA = StructType(
    [
        StructField("postcode", IntegerType(), True),
        StructField("region_name", StringType(), True),
        StructField("wind_risk_band", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingest_ts", TimestampType(), True),
        StructField("_rescued_data", StringType(), True),
    ]
)


def claim_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "claim_id": "CLM-001",
        "policy_id": "POL-001",
        "event_id": "CYC-001",
        "date_of_loss": "2025-03-10",
        "reported_date": "2025-03-15",
        "peril_type": "Wind",
        "claim_status": "Open",
        "incurred_amount": "10000",
        "paid_to_date": "2500",
        "snapshot_date": "2025-03-15",
        "_source_file": "claims_batch_01.csv",
        "_ingest_ts": datetime(2025, 1, 1, 0, 0, 0),
        "_rescued_data": None,
    }
    base.update(overrides)
    return base


def premium_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "policy_id": "POL-001",
        "insurer_name": "Pacific Rim Insurance",
        "postcode": 4870,
        "region_name": "Cairns QLD",
        "wind_risk_band": "T-W",
        "building_type": "Home",
        "sum_insured": "500000",
        "mitigation_flag": "None",
        "annual_premium": "1200",
        "policy_start_date": "2024-01-01",
        "policy_end_date": "2025-01-01",
        "_source_file": "premium_bordereau.csv",
        "_ingest_ts": datetime(2025, 1, 1, 0, 0, 0),
        "_rescued_data": None,
    }
    base.update(overrides)
    return base


def event_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "event_id": "CYC-001",
        "event_name": "Cyclone Test",
        "start_date": "2025-02-08",
        "end_date": "2025-02-14",
        "_source_file": "cyclone_events.csv",
        "_ingest_ts": datetime(2025, 1, 1, 0, 0, 0),
        "_rescued_data": None,
    }
    base.update(overrides)
    return base


def risk_zone_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "postcode": 4870,
        "region_name": "Cairns QLD",
        "wind_risk_band": "T-W",
        "_source_file": "risk_zone_lookup.csv",
        "_ingest_ts": datetime(2025, 1, 1, 0, 0, 0),
        "_rescued_data": None,
    }
    base.update(overrides)
    return base
