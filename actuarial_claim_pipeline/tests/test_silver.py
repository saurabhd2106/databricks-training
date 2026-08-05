"""Unit tests for silver transforms."""

from actuarial_claim_pipeline.data_quality import count_premiums_bad_dates
from actuarial_claim_pipeline.silver import (
    transform_silver_claims,
    transform_silver_cyclone_events,
    transform_silver_premiums,
    transform_silver_risk_zones,
)
from helpers import (
    CLAIMS_SCHEMA,
    EVENTS_SCHEMA,
    PREMIUMS_SCHEMA,
    RISK_ZONE_SCHEMA,
    claim_row,
    event_row,
    premium_row,
    risk_zone_row,
)


def test_silver_claims_keeps_valid_and_allows_null_event(spark):
    bronze = spark.createDataFrame(
        [
            claim_row(),
            claim_row(claim_id="CLM-002", event_id=None, peril_type="Flood", claim_status="Closed"),
        ],
        schema=CLAIMS_SCHEMA,
    )
    silver = transform_silver_claims(bronze)
    assert silver.count() == 2
    assert "bronze_ingestion_timestamp" in silver.columns
    assert "silver_ingestion_timestamp" in silver.columns
    assert "ingestion_timestamp" not in silver.columns
    row = silver.filter("claim_id = 'CLM-001'").collect()[0]
    assert float(row.incurred_amount) == 10000.0
    assert float(row.paid_to_date) == 2500.0


def test_silver_claims_drops_null_required(spark):
    bronze = spark.createDataFrame(
        [
            claim_row(claim_id="CLM-OK"),
            claim_row(claim_id="CLM-BAD", policy_id=None),
        ],
        schema=CLAIMS_SCHEMA,
    )
    assert transform_silver_claims(bronze).count() == 1


def test_silver_claims_drops_bad_business_rules(spark):
    bronze = spark.createDataFrame(
        [
            claim_row(claim_id="CLM-OK"),
            claim_row(claim_id="CLM-DATES", date_of_loss="2025-03-20", reported_date="2025-03-10"),
            claim_row(claim_id="CLM-NEG", incurred_amount="-1"),
            claim_row(claim_id="CLM-PAID", paid_to_date="20000"),
            claim_row(claim_id="CLM-PARSE", date_of_loss="not-a-date"),
        ],
        schema=CLAIMS_SCHEMA,
    )
    ids = {r.claim_id for r in transform_silver_claims(bronze).collect()}
    assert ids == {"CLM-OK"}


def test_silver_cyclone_parses_slash_and_dash_dates(spark):
    bronze = spark.createDataFrame(
        [
            event_row(event_id="CYC-1", start_date="2025/02/08", end_date="2025-02-14"),
            event_row(event_id="CYC-2", start_date="2025-11-20", end_date="2025-11-25"),
            event_row(event_id="CYC-BAD", start_date="2025-03-20", end_date="2025-03-10"),
            event_row(event_id="CYC-NULL", event_name=None),
        ],
        schema=EVENTS_SCHEMA,
    )
    silver = transform_silver_cyclone_events(bronze)
    ids = {r.event_id for r in silver.collect()}
    assert ids == {"CYC-1", "CYC-2"}


def test_silver_premiums_casts_and_filters(spark):
    bronze = spark.createDataFrame(
        [
            premium_row(policy_id="POL-OK"),
            premium_row(policy_id="POL-DATES", policy_start_date="2025-01-01", policy_end_date="2024-01-01"),
            premium_row(policy_id="POL-EQ", policy_start_date="2024-01-01", policy_end_date="2024-01-01"),
            premium_row(policy_id="POL-SI", sum_insured="0"),
            premium_row(policy_id="POL-PREM", annual_premium="-1"),
        ],
        schema=PREMIUMS_SCHEMA,
    )
    silver = transform_silver_premiums(bronze)
    assert silver.count() == 1
    row = silver.collect()[0]
    assert row.postcode == 4870
    assert float(row.sum_insured) == 500000.0


def test_silver_risk_zone_dedup_keeps_lowest_region_name(spark):
    bronze = spark.createDataFrame(
        [
            risk_zone_row(postcode=4870, region_name="Cairns QLD", wind_risk_band="T-W"),
            risk_zone_row(postcode=4870, region_name="Auckland NZ", wind_risk_band="Q-S"),
            risk_zone_row(postcode=800, region_name="Darwin NT", wind_risk_band="T-W"),
            risk_zone_row(postcode=None, region_name="Null Postcode", wind_risk_band="T-W"),
        ],
        schema=RISK_ZONE_SCHEMA,
    )
    silver = transform_silver_risk_zones(bronze)
    assert silver.count() == 2
    kept = silver.filter("postcode = 4870").collect()[0]
    assert kept.region_name == "Auckland NZ"
    assert kept.wind_risk_band == "Q-S"


def test_dq_vs_silver_premium_date_parity(spark):
    """DQ flags start >= end; silver enforces start < end (equal dates dropped only in silver)."""
    bronze = spark.createDataFrame(
        [premium_row(policy_id="POL-EQ", policy_start_date="2024-01-01", policy_end_date="2024-01-01")],
        schema=PREMIUMS_SCHEMA,
    )
    assert count_premiums_bad_dates(bronze) == 1
    assert transform_silver_premiums(bronze).count() == 0
