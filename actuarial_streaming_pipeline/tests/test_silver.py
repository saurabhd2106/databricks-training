"""Unit tests for silver typed cleanses (Databricks Connect)."""

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

from actuarial_streaming_pipeline.silver import (
    apply_claims_quality,
    apply_premiums_quality,
    transform_silver_claims_current,
    transform_silver_cyclone_events,
    transform_silver_risk_zones,
    transform_typed_claims,
    transform_typed_premiums,
)


def test_typed_claims_maps_audit_columns(spark):
    bronze = spark.createDataFrame([claim_row()], schema=CLAIMS_SCHEMA)
    typed = transform_typed_claims(bronze)
    assert "bronze_ingestion_timestamp" in typed.columns
    assert "source_file_name" in typed.columns
    assert "_ingest_ts" not in typed.columns


def test_typed_claims_try_cast_nulls_garbage(spark):
    bronze = spark.createDataFrame(
        [
            claim_row(claim_id="CLM-OK"),
            claim_row(
                claim_id="CLM-BAD-CAST",
                date_of_loss="not-a-date",
                incurred_amount="not-a-number",
            ),
        ],
        schema=CLAIMS_SCHEMA,
    )
    typed = transform_typed_claims(bronze)
    by_id = {r.claim_id: r for r in typed.collect()}

    ok = by_id["CLM-OK"]
    assert ok.date_of_loss is not None
    assert ok.incurred_amount is not None

    bad = by_id["CLM-BAD-CAST"]
    assert bad.date_of_loss is None
    assert bad.incurred_amount is None


def test_typed_premiums_maps_audit_columns(spark):
    bronze = spark.createDataFrame([premium_row()], schema=PREMIUMS_SCHEMA)
    typed = transform_typed_premiums(bronze)
    assert "bronze_ingestion_timestamp" in typed.columns
    assert "source_file_name" in typed.columns
    assert "_ingest_ts" not in typed.columns
    assert "_source_file" not in typed.columns


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


def test_apply_claims_quality_drops_paid_gt_incurred_and_negative(spark):
    bronze = spark.createDataFrame(
        [
            claim_row(claim_id="CLM-OK"),
            claim_row(claim_id="CLM-PAID", incurred_amount="100", paid_to_date="150"),
            claim_row(claim_id="CLM-NEG", incurred_amount="-10", paid_to_date="0"),
            claim_row(
                claim_id="CLM-UNPARSEABLE",
                date_of_loss="not-a-date",
            ),
        ],
        schema=CLAIMS_SCHEMA,
    )
    silver = apply_claims_quality(transform_typed_claims(bronze))
    assert {r.claim_id for r in silver.collect()} == {"CLM-OK"}


def test_claims_current_keeps_latest_snapshot(spark):
    snaps = apply_claims_quality(
        transform_typed_claims(
            spark.createDataFrame(
                [
                    claim_row(
                        claim_id="CLM-1",
                        snapshot_date="2025-01-01",
                        incurred_amount="100",
                        paid_to_date="50",
                    ),
                    claim_row(
                        claim_id="CLM-1",
                        snapshot_date="2025-02-01",
                        incurred_amount="200",
                        paid_to_date="50",
                    ),
                    claim_row(
                        claim_id="CLM-2",
                        snapshot_date="2025-01-15",
                        incurred_amount="50",
                        paid_to_date="10",
                    ),
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


def test_apply_premiums_quality_drops_bad_dates_and_sum_insured(spark):
    bronze = spark.createDataFrame(
        [
            premium_row(policy_id="POL-OK"),
            premium_row(
                policy_id="POL-DATES",
                policy_start_date="2025-01-01",
                policy_end_date="2025-01-01",
            ),
            premium_row(policy_id="POL-SUM", sum_insured="0"),
        ],
        schema=PREMIUMS_SCHEMA,
    )
    silver = apply_premiums_quality(transform_typed_premiums(bronze))
    rows = silver.collect()
    assert {r.policy_id for r in rows} == {"POL-OK"}
    assert "silver_ingestion_timestamp" in silver.columns
    assert rows[0].silver_ingestion_timestamp is not None


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
