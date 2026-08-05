"""Unit tests for silver typed cleanses (Databricks Connect)."""

from actuarial_claim_streaming_pipeline.silver import (
    apply_claims_quality,
    apply_premiums_quality,
    transform_silver_claims_current,
    transform_silver_cyclone_events,
    transform_silver_risk_zones,
    transform_typed_claims,
    transform_typed_premiums,
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


def test_typed_claims_maps_audit_columns(spark):
    bronze = spark.createDataFrame([claim_row()], schema=CLAIMS_SCHEMA)
    typed = transform_typed_claims(bronze)
    assert "bronze_ingestion_timestamp" in typed.columns
    assert "source_file_name" in typed.columns
    assert "_ingest_ts" not in typed.columns


def test_apply_claims_quality_drops_bad_rows(spark):
    bronze = spark.createDataFrame(
        [
            claim_row(claim_id="CLM-OK"),
            claim_row(claim_id="CLM-BAD", policy_id=None),
            claim_row(claim_id="CLM-DATES", date_of_loss="2025-03-20", reported_date="2025-03-10"),
        ],
        schema=CLAIMS_SCHEMA,
    )
    silver = apply_claims_quality(transform_typed_claims(bronze))
    ids = {r.claim_id for r in silver.collect()}
    assert ids == {"CLM-OK"}
    assert "silver_ingestion_timestamp" in silver.columns


def test_claims_current_keeps_latest_snapshot(spark):
    snaps = apply_claims_quality(
        transform_typed_claims(
            spark.createDataFrame(
                [
                    claim_row(claim_id="CLM-1", snapshot_date="2025-01-01", incurred_amount="100"),
                    claim_row(claim_id="CLM-1", snapshot_date="2025-02-01", incurred_amount="200"),
                    claim_row(claim_id="CLM-2", snapshot_date="2025-01-15", incurred_amount="50"),
                ],
                schema=CLAIMS_SCHEMA,
            )
        )
    )
    current = transform_silver_claims_current(snaps)
    rows = {r.claim_id: float(r.incurred_amount) for r in current.collect()}
    assert rows == {"CLM-1": 200.0, "CLM-2": 50.0}


def test_typed_premiums_and_quality(spark):
    bronze = spark.createDataFrame(
        [
            premium_row(policy_id="POL-OK"),
            premium_row(policy_id="POL-BAD", annual_premium="-1"),
        ],
        schema=PREMIUMS_SCHEMA,
    )
    silver = apply_premiums_quality(transform_typed_premiums(bronze))
    assert {r.policy_id for r in silver.collect()} == {"POL-OK"}


def test_cyclone_and_risk_zone_transforms(spark):
    events = transform_silver_cyclone_events(
        spark.createDataFrame(
            [
                event_row(event_id="CYC-1", start_date="2025/02/08", end_date="2025-02-14"),
                event_row(event_id="CYC-BAD", start_date="2025-03-20", end_date="2025-03-10"),
            ],
            schema=EVENTS_SCHEMA,
        )
    )
    assert {r.event_id for r in events.collect()} == {"CYC-1"}

    zones = transform_silver_risk_zones(
        spark.createDataFrame(
            [
                risk_zone_row(postcode=4870, region_name="B Region"),
                risk_zone_row(postcode=4870, region_name="A Region"),
            ],
            schema=RISK_ZONE_SCHEMA,
        )
    )
    assert zones.count() == 1
    assert zones.collect()[0].region_name == "A Region"
