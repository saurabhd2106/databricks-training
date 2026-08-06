"""Unit tests for leakage-safe claim severity features."""

import pandas as pd

from ml_pipeline_demo.features import build_claim_severity_features


def _claims_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "claim_id": "CLM-1",
                "policy_id": "POL-1",
                "event_id": "CYC-1",
                "date_of_loss": "2025-02-10",
                "reported_date": "2025-02-17",
                "peril_type": "Wind",
                "claim_status": "Open",
                "incurred_amount": "10000",
                "paid_to_date": "0",
                "snapshot_date": "2025-02-17",
            },
            {
                "claim_id": "CLM-1",
                "policy_id": "POL-1",
                "event_id": "CYC-1",
                "date_of_loss": "2025-02-10",
                "reported_date": "2025-02-17",
                "peril_type": "Wind",
                "claim_status": "Closed",
                "incurred_amount": "22000",
                "paid_to_date": "20000",
                "snapshot_date": "2025-03-01",
            },
            {
                "claim_id": "CLM-2",
                "policy_id": "POL-2",
                "event_id": "CYC-1",
                "date_of_loss": "2025-02-11",
                "reported_date": "2025-02-12",
                "peril_type": "Riverine Flood",
                "claim_status": "Open",
                "incurred_amount": "5000",
                "paid_to_date": "0",
                "snapshot_date": "2025-02-12",
            },
            {
                "claim_id": "CLM-3",
                "policy_id": "POL-1",
                "event_id": "CYC-1",
                "date_of_loss": "2025-02-10",
                "reported_date": "2025-02-11",
                "peril_type": "Wind",
                "claim_status": "Closed",
                "incurred_amount": "N/A",
                "paid_to_date": "0",
                "snapshot_date": "2025-02-20",
            },
        ]
    )


def _policies_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "policy_id": "POL-1",
                "wind_risk_band": "T-W",
                "building_type": "Home",
                "sum_insured": "400000",
                "mitigation_flag": "None",
                "region_name": "Cairns QLD",
            },
            {
                "policy_id": "POL-2",
                "wind_risk_band": "B-F",
                "building_type": "SME",
                "sum_insured": "800000",
                "mitigation_flag": "Roof Tie-Down",
                "region_name": "Perth WA",
            },
        ]
    )


def test_builds_one_row_per_closed_claim_with_first_incurred():
    features = build_claim_severity_features(_claims_frame(), _policies_frame())

    assert len(features) == 1
    row = features.iloc[0]
    assert row["claim_id"] == "CLM-1"
    assert row["first_incurred"] == 10000.0
    assert row["ultimate_incurred"] == 22000.0
    assert row["report_lag_days"] == 7
    assert row["peril_type"] == "Wind"
    assert row["sum_insured"] == 400000.0


def test_excludes_still_open_and_non_numeric_ultimate():
    features = build_claim_severity_features(_claims_frame(), _policies_frame())
    assert set(features["claim_id"]) == {"CLM-1"}


def test_does_not_use_later_paid_as_feature_column():
    features = build_claim_severity_features(_claims_frame(), _policies_frame())
    assert "paid_to_date" not in features.columns
