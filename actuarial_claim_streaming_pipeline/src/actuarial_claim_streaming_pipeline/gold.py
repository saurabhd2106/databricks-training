"""Gold-layer actuarial marts for streaming pipeline Materialized Views."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_gold_claims_summary(claims: DataFrame, premiums: DataFrame) -> DataFrame:
    """Claims frequency, severity and settlement by peril/status/region dims."""
    return (
        claims.alias("c")
        .join(premiums.alias("p"), F.col("c.policy_id") == F.col("p.policy_id"), "left")
        .groupBy(
            F.col("c.peril_type"),
            F.col("c.claim_status"),
            F.col("p.region_name"),
            F.col("p.wind_risk_band"),
            F.col("p.building_type"),
        )
        .agg(
            F.count("c.claim_id").alias("claim_count"),
            F.sum("c.incurred_amount").alias("total_incurred"),
            F.sum("c.paid_to_date").alias("total_paid"),
            F.sum(F.col("c.incurred_amount") - F.col("c.paid_to_date")).alias("outstanding_reserve"),
            F.round(F.avg("c.incurred_amount"), 2).alias("avg_claim_severity"),
            F.round(
                F.sum("c.paid_to_date") / F.nullif(F.sum("c.incurred_amount"), F.lit(0)) * 100,
                2,
            ).alias("settlement_pct"),
            F.current_timestamp().alias("gold_ingestion_timestamp"),
        )
    )


def build_gold_loss_ratio_by_risk(premiums: DataFrame, claims: DataFrame) -> DataFrame:
    """Loss ratio by insurer, region, risk band, building type, mitigation."""
    return (
        premiums.alias("p")
        .join(claims.alias("c"), F.col("p.policy_id") == F.col("c.policy_id"), "left")
        .groupBy(
            F.col("p.insurer_name"),
            F.col("p.region_name"),
            F.col("p.wind_risk_band"),
            F.col("p.building_type"),
            F.col("p.mitigation_flag"),
        )
        .agg(
            F.countDistinct("p.policy_id").alias("policy_count"),
            F.sum("p.annual_premium").alias("total_premium"),
            F.count("c.claim_id").alias("claim_count"),
            F.coalesce(F.sum("c.incurred_amount"), F.lit(0)).alias("total_incurred"),
            F.round(
                F.coalesce(F.sum("c.incurred_amount"), F.lit(0))
                / F.nullif(F.sum("p.annual_premium"), F.lit(0))
                * 100,
                2,
            ).alias("loss_ratio_pct"),
            F.current_timestamp().alias("gold_ingestion_timestamp"),
        )
    )


def build_gold_event_loss_summary(
    claims: DataFrame,
    events: DataFrame,
    premiums: DataFrame,
) -> DataFrame:
    """Cat vs non-cat loss aggregation per named cyclone event."""
    return (
        claims.alias("c")
        .join(events.alias("e"), F.col("c.event_id") == F.col("e.event_id"), "left")
        .join(premiums.alias("p"), F.col("c.policy_id") == F.col("p.policy_id"), "left")
        .groupBy(
            F.when(F.col("c.event_id").isNotNull(), F.lit("Catastrophe"))
            .otherwise(F.lit("Non-Catastrophe"))
            .alias("claim_category"),
            F.col("e.event_name"),
            F.col("e.start_date").alias("event_start"),
            F.col("e.end_date").alias("event_end"),
            F.datediff(F.col("e.end_date"), F.col("e.start_date")).alias("event_duration_days"),
            F.col("p.region_name"),
            F.col("c.peril_type"),
        )
        .agg(
            F.count("c.claim_id").alias("claim_count"),
            F.sum("c.incurred_amount").alias("total_incurred"),
            F.round(F.avg("c.incurred_amount"), 2).alias("avg_claim_severity"),
            F.max("c.incurred_amount").alias("max_claim_severity"),
            F.sum("c.paid_to_date").alias("total_paid"),
            F.sum(F.col("c.incurred_amount") - F.col("c.paid_to_date")).alias("outstanding_reserve"),
            F.current_timestamp().alias("gold_ingestion_timestamp"),
        )
    )


def build_gold_portfolio_exposure(premiums: DataFrame, risk_zones: DataFrame) -> DataFrame:
    """Portfolio exposure via INNER JOIN on postcode."""
    return (
        premiums.alias("p")
        .join(risk_zones.alias("rz"), F.col("p.postcode") == F.col("rz.postcode"), "inner")
        .groupBy(
            F.col("p.insurer_name"),
            F.col("p.region_name"),
            F.col("p.wind_risk_band"),
            F.col("p.building_type"),
            F.col("p.mitigation_flag"),
        )
        .agg(
            F.countDistinct("p.policy_id").alias("policy_count"),
            F.sum("p.sum_insured").alias("total_sum_insured"),
            F.sum("p.annual_premium").alias("total_annual_premium"),
            F.round(F.avg("p.sum_insured"), 2).alias("avg_sum_insured"),
            F.round(F.avg("p.annual_premium"), 2).alias("avg_annual_premium"),
            F.round(
                F.sum("p.annual_premium") / F.nullif(F.sum("p.sum_insured"), F.lit(0)) * 100,
                4,
            ).alias("premium_rate_pct"),
            F.current_timestamp().alias("gold_ingestion_timestamp"),
        )
    )


def build_gold_claims_development(claims: DataFrame, premiums: DataFrame) -> DataFrame:
    """Reporting lag, IBNR indicators and reserve by peril and month."""
    return (
        claims.alias("c")
        .join(premiums.alias("p"), F.col("c.policy_id") == F.col("p.policy_id"), "left")
        .groupBy(
            F.col("c.peril_type"),
            F.col("p.region_name"),
            F.col("p.wind_risk_band"),
            F.date_trunc("month", F.col("c.date_of_loss")).alias("loss_month"),
            F.date_trunc("month", F.col("c.reported_date")).alias("reported_month"),
        )
        .agg(
            F.count("c.claim_id").alias("claim_count"),
            F.round(F.avg(F.datediff(F.col("c.reported_date"), F.col("c.date_of_loss"))), 1).alias(
                "avg_reporting_lag_days"
            ),
            F.max(F.datediff(F.col("c.reported_date"), F.col("c.date_of_loss"))).alias(
                "max_reporting_lag_days"
            ),
            F.sum("c.incurred_amount").alias("total_incurred"),
            F.sum("c.paid_to_date").alias("total_paid"),
            F.sum(F.col("c.incurred_amount") - F.col("c.paid_to_date")).alias("outstanding_reserve"),
            F.round(
                F.sum("c.paid_to_date") / F.nullif(F.sum("c.incurred_amount"), F.lit(0)) * 100,
                2,
            ).alias("payment_progress_pct"),
            F.current_timestamp().alias("gold_ingestion_timestamp"),
        )
    )
