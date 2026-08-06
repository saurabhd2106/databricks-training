"""Silver-layer typed cleanses for streaming pipeline Materialized Views / temp views."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def transform_typed_claims(bronze: DataFrame) -> DataFrame:
    """Type claims columns and map bronze audit fields (no quality filters)."""
    return bronze.select(
        F.col("claim_id"),
        F.col("policy_id"),
        F.col("event_id"),
        F.expr("try_cast(date_of_loss AS DATE)").alias("date_of_loss"),
        F.expr("try_cast(reported_date AS DATE)").alias("reported_date"),
        F.col("peril_type"),
        F.col("claim_status"),
        F.expr("try_cast(incurred_amount AS DECIMAL(18,2))").alias("incurred_amount"),
        F.expr("try_cast(paid_to_date AS DECIMAL(18,2))").alias("paid_to_date"),
        F.expr("try_cast(snapshot_date AS DATE)").alias("snapshot_date"),
        F.col("_source_file").alias("source_file_name"),
        F.col("_ingest_ts").alias("bronze_ingestion_timestamp"),
    )


def apply_claims_quality(typed: DataFrame) -> DataFrame:
    """Filter typed claims to silver-quality rows and stamp silver ingest time."""
    return (
        typed.filter(F.col("claim_id").isNotNull())
        .filter(F.col("policy_id").isNotNull())
        .filter(F.col("date_of_loss").isNotNull())
        .filter(F.col("reported_date").isNotNull())
        .filter(F.col("peril_type").isNotNull())
        .filter(F.col("claim_status").isNotNull())
        .filter(F.col("incurred_amount").isNotNull())
        .filter(F.col("paid_to_date").isNotNull())
        .filter(F.col("date_of_loss") <= F.col("reported_date"))
        .filter(F.col("incurred_amount") >= 0)
        .filter(F.col("paid_to_date") <= F.col("incurred_amount"))
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    )


def transform_typed_premiums(bronze: DataFrame) -> DataFrame:
    """Type premium columns and map bronze audit fields (no quality filters)."""
    return bronze.select(
        F.col("policy_id"),
        F.col("insurer_name"),
        F.expr("try_cast(postcode AS INT)").alias("postcode"),
        F.col("region_name"),
        F.col("wind_risk_band"),
        F.col("building_type"),
        F.expr("try_cast(sum_insured AS DECIMAL(18,2))").alias("sum_insured"),
        F.col("mitigation_flag"),
        F.expr("try_cast(annual_premium AS DECIMAL(18,2))").alias("annual_premium"),
        F.expr("try_cast(policy_start_date AS DATE)").alias("policy_start_date"),
        F.expr("try_cast(policy_end_date AS DATE)").alias("policy_end_date"),
        F.col("_source_file").alias("source_file_name"),
        F.col("_ingest_ts").alias("bronze_ingestion_timestamp"),
    )


def apply_premiums_quality(typed: DataFrame) -> DataFrame:
    """Filter typed premiums to silver-quality rows and stamp silver ingest time."""
    return (
        typed.filter(F.col("policy_id").isNotNull())
        .filter(F.col("insurer_name").isNotNull())
        .filter(F.col("postcode").isNotNull())
        .filter(F.col("region_name").isNotNull())
        .filter(F.col("wind_risk_band").isNotNull())
        .filter(F.col("building_type").isNotNull())
        .filter(F.col("sum_insured").isNotNull())
        .filter(F.col("mitigation_flag").isNotNull())
        .filter(F.col("annual_premium").isNotNull())
        .filter(F.col("policy_start_date").isNotNull())
        .filter(F.col("policy_end_date").isNotNull())
        .filter(F.col("policy_start_date") < F.col("policy_end_date"))
        .filter(F.col("sum_insured") > 0)
        .filter(F.col("annual_premium") > 0)
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    )


def transform_silver_cyclone_events(bronze: DataFrame) -> DataFrame:
    """Type and filter cyclone events from clean bronze."""
    start_date = F.coalesce(
        F.expr("try_to_date(start_date, 'yyyy/MM/dd')"),
        F.expr("try_to_date(start_date, 'yyyy-MM-dd')"),
        F.expr("try_cast(start_date AS DATE)"),
    )
    end_date = F.coalesce(
        F.expr("try_to_date(end_date, 'yyyy/MM/dd')"),
        F.expr("try_to_date(end_date, 'yyyy-MM-dd')"),
        F.expr("try_cast(end_date AS DATE)"),
    )
    return (
        bronze.select(
            F.col("event_id"),
            F.col("event_name"),
            start_date.alias("start_date"),
            end_date.alias("end_date"),
            F.col("_source_file").alias("source_file_name"),
            F.col("_ingest_ts").alias("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp"),
        )
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_name").isNotNull())
        .filter(F.col("start_date").isNotNull())
        .filter(F.col("end_date").isNotNull())
        .filter(F.col("start_date") <= F.col("end_date"))
    )


def transform_silver_risk_zones(bronze: DataFrame) -> DataFrame:
    """Drop nulls and keep one row per postcode (lowest region_name alphabetically)."""
    filtered = bronze.filter(
        F.col("postcode").isNotNull()
        & F.col("region_name").isNotNull()
        & F.col("wind_risk_band").isNotNull()
    )
    w = Window.partitionBy("postcode").orderBy(F.col("region_name").asc())
    return (
        filtered.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("postcode"),
            F.col("region_name"),
            F.col("wind_risk_band"),
            F.col("_source_file").alias("source_file_name"),
            F.col("_ingest_ts").alias("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp"),
        )
    )


def transform_silver_claims_current(snapshots: DataFrame) -> DataFrame:
    """Latest claim snapshot per claim_id."""
    w = Window.partitionBy("claim_id").orderBy(F.col("snapshot_date").desc())
    return (
        snapshots.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
