"""Silver-layer DataFrame transforms (typed, filtered, deduplicated)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def transform_silver_claims(bronze: DataFrame) -> DataFrame:
    """Type, filter, and lineage-stamp claims bordereau rows."""
    date_of_loss = F.expr("try_cast(date_of_loss AS DATE)")
    reported_date = F.expr("try_cast(reported_date AS DATE)")
    incurred = F.expr("try_cast(incurred_amount AS DECIMAL(18,2))")
    paid = F.expr("try_cast(paid_to_date AS DECIMAL(18,2))")

    return (
        bronze.select(
            F.col("claim_id"),
            F.col("policy_id"),
            F.col("event_id"),
            date_of_loss.alias("date_of_loss"),
            reported_date.alias("reported_date"),
            F.col("peril_type"),
            F.col("claim_status"),
            incurred.alias("incurred_amount"),
            paid.alias("paid_to_date"),
            F.expr("try_cast(snapshot_date AS DATE)").alias("snapshot_date"),
            F.col("source_file_name"),
            F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp"),
        )
        .filter(F.col("claim_id").isNotNull())
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
    )


def transform_silver_cyclone_events(bronze: DataFrame) -> DataFrame:
    """Type, filter cyclone events. start_date supports yyyy/MM/dd and yyyy-MM-dd."""
    start_date = F.coalesce(
        F.expr("try_to_date(start_date, 'yyyy/MM/dd')"),
        F.expr("try_to_date(start_date, 'yyyy-MM-dd')"),
    )

    return (
        bronze.select(
            F.col("event_id"),
            F.col("event_name"),
            start_date.alias("start_date"),
            F.col("end_date"),
            F.col("source_file_name"),
            F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp"),
        )
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_name").isNotNull())
        .filter(F.col("start_date").isNotNull())
        .filter(F.col("end_date").isNotNull())
        .filter(F.col("start_date") <= F.col("end_date"))
    )


def transform_silver_premiums(bronze: DataFrame) -> DataFrame:
    """Type, filter premium bordereau rows."""
    return (
        bronze.select(
            F.col("policy_id"),
            F.col("insurer_name"),
            F.expr("TRY_CAST(postcode AS INT)").alias("postcode"),
            F.col("region_name"),
            F.col("wind_risk_band"),
            F.col("building_type"),
            F.expr("TRY_CAST(sum_insured AS DECIMAL(18,2))").alias("sum_insured"),
            F.col("mitigation_flag"),
            F.expr("TRY_CAST(annual_premium AS DECIMAL(18,2))").alias("annual_premium"),
            F.expr("TRY_CAST(policy_start_date AS DATE)").alias("policy_start_date"),
            F.expr("TRY_CAST(policy_end_date AS DATE)").alias("policy_end_date"),
            F.col("source_file_name"),
            F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp"),
        )
        .filter(F.col("policy_id").isNotNull())
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
            F.col("source_file_name"),
            F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp"),
        )
    )


def build_silver_tables(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> dict[str, DataFrame]:
    """Read bronze tables, transform to silver DataFrames, and persist them."""
    claims = transform_silver_claims(spark.table(f"{catalog}.{schema}.bronze_claims_bordereau"))
    events = transform_silver_cyclone_events(spark.table(f"{catalog}.{schema}.bronze_cyclone_events"))
    premiums = transform_silver_premiums(spark.table(f"{catalog}.{schema}.bronze_premium_bordereau"))
    risk_zones = transform_silver_risk_zones(spark.table(f"{catalog}.{schema}.bronze_risk_zone_lookup"))

    claims.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.silver_claims_bordereau"
    )
    events.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.silver_cyclone_events"
    )
    premiums.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.silver_premium_bordereau"
    )
    risk_zones.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.silver_risk_zone_lookup"
    )

    return {
        "silver_claims_bordereau": claims,
        "silver_cyclone_events": events,
        "silver_premium_bordereau": premiums,
        "silver_risk_zone_lookup": risk_zones,
    }
