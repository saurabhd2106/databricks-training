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


def with_audit(spark: SparkSession, rows: list[dict[str, Any]], schema: StructType | None = None) -> DataFrame:
    """Create a DataFrame and attach bronze audit columns if missing."""
    df = spark.createDataFrame(rows, schema=schema) if schema else spark.createDataFrame(rows)
    cols = set(df.columns)
    if "ingestion_timestamp" not in cols:
        df = df.withColumn("ingestion_timestamp", F.lit(datetime(2025, 1, 1, 0, 0, 0)))
    if "source_file_name" not in cols:
        df = df.withColumn("source_file_name", F.lit("fixture.csv"))
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
        StructField("source_file_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)

EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)

PREMIUMS_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), True),
        StructField("insurer_name", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("wind_risk_band", StringType(), True),
        StructField("building_type", StringType(), True),
        StructField("sum_insured", StringType(), True),
        StructField("mitigation_flag", StringType(), True),
        StructField("annual_premium", StringType(), True),
        StructField("policy_start_date", StringType(), True),
        StructField("policy_end_date", StringType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)

RISK_ZONE_SCHEMA = StructType(
    [
        StructField("postcode", IntegerType(), True),
        StructField("region_name", StringType(), True),
        StructField("wind_risk_band", StringType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)


def claim_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "claim_id": "CLM-001",
        "policy_id": "POL-001",
        "event_id": "CYC-2025-01",
        "date_of_loss": "2025-02-10",
        "reported_date": "2025-02-17",
        "peril_type": "Wind",
        "claim_status": "Open",
        "incurred_amount": "10000.00",
        "paid_to_date": "2500.00",
        "snapshot_date": "2025-02-17",
        "source_file_name": "claims.csv",
        "ingestion_timestamp": datetime(2025, 1, 1),
    }
    base.update(overrides)
    return base


def event_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "event_id": "CYC-2025-01",
        "event_name": "Cyclone Alpha",
        "start_date": "2025/02/08",
        "end_date": "2025-02-14",
        "source_file_name": "events.csv",
        "ingestion_timestamp": datetime(2025, 1, 1),
    }
    base.update(overrides)
    return base


def premium_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "policy_id": "POL-001",
        "insurer_name": "Pacific Rim Insurance",
        "postcode": "4870",
        "region_name": "Cairns QLD",
        "wind_risk_band": "T-W",
        "building_type": "Home",
        "sum_insured": "500000.00",
        "mitigation_flag": "Shutters Installed",
        "annual_premium": "2500.00",
        "policy_start_date": "2024-01-01",
        "policy_end_date": "2025-01-01",
        "source_file_name": "premiums.csv",
        "ingestion_timestamp": datetime(2025, 1, 1),
    }
    base.update(overrides)
    return base


def risk_zone_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "postcode": 4870,
        "region_name": "Cairns QLD",
        "wind_risk_band": "T-W",
        "source_file_name": "risk_zone.csv",
        "ingestion_timestamp": datetime(2025, 1, 1),
    }
    base.update(overrides)
    return base
