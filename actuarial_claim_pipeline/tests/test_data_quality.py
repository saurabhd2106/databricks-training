"""Unit tests for bronze data-quality checks."""

from datetime import datetime

from actuarial_claim_pipeline.data_quality import (
    business_rule_violations,
    duplicate_summary,
    find_duplicates,
    null_analysis,
    null_severity,
    referential_integrity_violations,
    risk_zone_uniqueness,
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


def test_duplicates_flagged_excluding_audit_cols(spark):
    rows = [
        claim_row(ingestion_timestamp=datetime(2025, 1, 1)),
        claim_row(ingestion_timestamp=datetime(2025, 1, 2)),  # same business cols, different audit
    ]
    df = spark.createDataFrame(rows, schema=CLAIMS_SCHEMA)
    summary = duplicate_summary(df)
    assert summary["dupe_groups"] == 1
    assert summary["dupe_rows"] == 2
    assert summary["rows_to_drop"] == 1
    assert find_duplicates(df).count() == 1


def test_clean_table_has_zero_duplicates(spark):
    df = spark.createDataFrame(
        [claim_row(claim_id="CLM-001"), claim_row(claim_id="CLM-002")],
        schema=CLAIMS_SCHEMA,
    )
    assert duplicate_summary(df)["dupe_groups"] == 0


def test_null_severity_thresholds():
    assert null_severity(10.0, 10.0) == "MEDIUM"
    assert null_severity(10.1, 10.0) == "HIGH"
    assert null_severity(5.0, 10.0) == "MEDIUM"


def test_null_analysis_severity(spark):
    # 1 of 10 rows null in policy_id => 10% => MEDIUM (not > 10)
    rows = [claim_row(claim_id=f"CLM-{i}", policy_id="POL-001") for i in range(9)]
    rows.append(claim_row(claim_id="CLM-NULL", policy_id=None))
    df = spark.createDataFrame(rows, schema=CLAIMS_SCHEMA)
    result = {r["column"]: r for r in null_analysis(df, high_threshold_pct=10.0)}
    assert result["policy_id"]["severity"] == "MEDIUM"

    # 2 of 10 => 20% => HIGH
    rows[8] = claim_row(claim_id="CLM-NULL-2", policy_id=None)
    df2 = spark.createDataFrame(rows, schema=CLAIMS_SCHEMA)
    result2 = {r["column"]: r for r in null_analysis(df2, high_threshold_pct=10.0)}
    assert result2["policy_id"]["severity"] == "HIGH"


def test_business_rule_violations(spark):
    claims = spark.createDataFrame(
        [
            claim_row(),  # clean
            claim_row(claim_id="CLM-D", date_of_loss="2025-03-20", reported_date="2025-03-10"),
            claim_row(claim_id="CLM-N", incurred_amount="-1", paid_to_date="-1"),
            claim_row(claim_id="CLM-P", paid_to_date="20000.00"),
        ],
        schema=CLAIMS_SCHEMA,
    )
    events = spark.createDataFrame(
        [
            event_row(),
            event_row(event_id="CYC-BAD", start_date="2025-03-20", end_date="2025-03-10"),
        ],
        schema=EVENTS_SCHEMA,
    )
    premiums = spark.createDataFrame(
        [
            premium_row(),
            premium_row(policy_id="POL-BAD-D", policy_start_date="2025-01-01", policy_end_date="2024-01-01"),
            premium_row(policy_id="POL-BAD-S", sum_insured="0"),
            premium_row(policy_id="POL-BAD-A", annual_premium="-5"),
        ],
        schema=PREMIUMS_SCHEMA,
    )
    rules = business_rule_violations(claims, events, premiums)
    assert rules["claims_loss_after_reported"] == 1
    assert rules["claims_negative_incurred"] == 1
    assert rules["claims_paid_exceeds_incurred"] == 1
    assert rules["premiums_bad_dates"] == 1
    assert rules["premiums_non_positive_sum_insured"] == 1
    assert rules["premiums_non_positive_annual_premium"] == 1
    assert rules["events_start_after_end"] == 1


def test_referential_integrity(spark):
    claims = spark.createDataFrame(
        [
            claim_row(),  # ok
            claim_row(claim_id="CLM-OP", policy_id="POL-MISSING"),
            claim_row(claim_id="CLM-OE", event_id="CYC-MISSING"),
            claim_row(claim_id="CLM-NC", event_id=None),  # null event allowed
        ],
        schema=CLAIMS_SCHEMA,
    )
    events = spark.createDataFrame([event_row()], schema=EVENTS_SCHEMA)
    premiums = spark.createDataFrame(
        [premium_row(), premium_row(policy_id="POL-003", postcode="9999")],
        schema=PREMIUMS_SCHEMA,
    )
    risk_zone = spark.createDataFrame([risk_zone_row()], schema=RISK_ZONE_SCHEMA)

    ri = referential_integrity_violations(claims, events, premiums, risk_zone)
    assert ri["orphan_claim_policies"] == 1
    assert ri["orphan_claim_events"] == 1
    assert ri["orphan_premium_postcodes"] == 1


def test_risk_zone_uniqueness(spark):
    clean = spark.createDataFrame(
        [risk_zone_row(postcode=4870), risk_zone_row(postcode=800, region_name="Darwin NT")],
        schema=RISK_ZONE_SCHEMA,
    )
    assert risk_zone_uniqueness(clean)["is_unique"] is True

    dirty = spark.createDataFrame(
        [
            risk_zone_row(postcode=4870, region_name="Cairns QLD"),
            risk_zone_row(postcode=4870, region_name="Auckland NZ", wind_risk_band="Q-S"),
        ],
        schema=RISK_ZONE_SCHEMA,
    )
    result = risk_zone_uniqueness(dirty)
    assert result["is_unique"] is False
    assert result["extra_rows"] == 1
