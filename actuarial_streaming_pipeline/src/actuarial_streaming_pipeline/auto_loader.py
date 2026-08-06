"""Shared Auto Loader helpers for streaming bronze ingest.

Lakeflow manages schema location and checkpoints — do not set
cloudFiles.schemaLocation or checkpointLocation here.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_LANDING_PATH = "/Volumes/actuarial/dev/landing"

# Known actuarial CSV contracts (schemaHints). Remaining columns may still be inferred.
CLAIMS_SCHEMA_HINTS = (
    "claim_id STRING, policy_id STRING, event_id STRING, "
    "date_of_loss DATE, reported_date DATE, peril_type STRING, claim_status STRING, "
    "incurred_amount DECIMAL(18,2), paid_to_date DECIMAL(18,2), snapshot_date DATE"
)

PREMIUMS_SCHEMA_HINTS = (
    "policy_id STRING, insurer_name STRING, postcode INT, region_name STRING, "
    "wind_risk_band STRING, building_type STRING, sum_insured DECIMAL(18,2), "
    "mitigation_flag STRING, annual_premium DECIMAL(18,2), "
    "policy_start_date DATE, policy_end_date DATE"
)

RISK_ZONES_SCHEMA_HINTS = "postcode INT, region_name STRING, wind_risk_band STRING"

CYCLONE_EVENTS_SCHEMA_HINTS = (
    "event_id STRING, event_name STRING, start_date DATE, end_date DATE"
)


def landing_path(spark: SparkSession) -> str:
    """Resolve UC Volume landing root from pipeline configuration."""
    return spark.conf.get("landing_path", DEFAULT_LANDING_PATH).rstrip("/")


def read_landing_csv(
    spark: SparkSession,
    subdir: str,
    *,
    schema_hints: str | None = None,
) -> DataFrame:
    """Stream CSV files from a landing subdirectory via Auto Loader."""
    path = f"{landing_path(spark)}/{subdir}"
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)

    return (
        reader.load(path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def rescued_or_null_key(key_col: str) -> Column:
    """Rows that fail bronze ingest quality (rescued data or null business key)."""
    return F.col("_rescued_data").isNotNull() | F.col(key_col).isNull()


def quarantine_reason(key_col: str) -> Column:
    """Human-readable reason string for quarantine rows."""
    rescued = F.when(F.col("_rescued_data").isNotNull(), F.lit("rescued_data"))
    null_key = F.when(F.col(key_col).isNull(), F.lit(f"{key_col}_null"))
    return F.concat_ws(",", rescued, null_key)


def quarantine_from_raw(raw: DataFrame, key_col: str) -> DataFrame:
    """Filter invalid rows from a bronze raw stream and stamp quarantine metadata."""
    return (
        raw.filter(rescued_or_null_key(key_col))
        .withColumn("quarantine_reason", quarantine_reason(key_col))
        .withColumn("_quarantine_ts", F.current_timestamp())
    )
